# Aula 07 — De decomposição para threads: granularidade, balanceamento de carga, localidade e `ExecutorService`

> **Computação Escalável (2COP022)** · Profa. Larissa C. Shimomura · Aula de **25/09**
> **Tópicos:** task × thread × chunk · granularidade · número de threads · balanceamento estático e dinâmico · mapeamento (blocos, cíclico, aleatório, grafos, Master/Slave) · load balancer · localidade de dados · `Thread`/`Runnable` · `ExecutorService` · `Callable` · `Future` · introdução ao `ForkJoin`
> **Pré-requisitos:** [Aula 06](06_padroes_de_paralelismo.md) (decomposição de tarefas e dados, padrões)
> **Tempo estimado de estudo:** 3 h

> **[P2] O que a Larissa disse na revisão** ([orientações completas](12b_orientacoes_prova2.md))
> - **Cai:** trade-off entre **granularidade** das tarefas e **número de threads** (dissertativa; ver também o exemplo A/B/C da revisão).
> - **Cai:** balanceamento **estático × dinâmico** (conceitual); *work stealing* é **dinâmico**.
> - **Cai como interpretação de código:** dado um programa com `ExecutorService`, dizer **quantas tarefas**, **quantas threads** e **qual decomposição**. **Não vai pedir para programar.**
> - Não citados na revisão (continuam na lista de tópicos, prioridade menor): localidade de dados, load balancer (Round Robin × Least Connection), tipos de pool.

---

## Sumário

1. [Mapa da aula](#mapa-da-aula)
2. [De decomposição para threads](#1-de-decomposição-para-threads)
3. [Task, thread e chunk](#2-task-thread-e-chunk)
4. [Granularidade, número de tasks e de threads](#3-granularidade-número-de-tasks-e-número-de-threads)
5. [Balanceamento de carga: estático × dinâmico](#4-balanceamento-de-carga-estático--dinâmico)
6. [Mapeamento estático](#5-mapeamento-estático-como-dividir-os-dados)
7. [Mapeamento dinâmico e load balancer](#6-mapeamento-dinâmico-e-load-balancer)
8. [Localidade de dados](#7-localidade-de-dados-data-locality)
9. [Java: Thread e Runnable](#8-java-thread-e-runnable)
10. [Java: ExecutorService](#9-java-executorservice)
11. [Java: Callable e Future](#10-java-callable-e-future)
12. [Java: introdução ao ForkJoin](#11-java-introdução-ao-forkjoin)
13. [Exercício 1 resolvido](#12-exercício-1-resolvido-para-leitura)
14. [Treino de leitura de código](#13-treino-de-leitura-de-código)
15. [Erros comuns / o que cai em prova](#14-erros-comuns--o-que-cai-em-prova)
16. [Fontes](#15-fontes)
17. [Conclusão para revisão](#16-conclusão-para-revisão)

---

## Mapa da aula

```mermaid
flowchart LR
    A["Aula 06:<br/>decomposição de<br/>dados e tarefas"] --> B["Quantas threads?<br/>Qual granularidade?"]
    B --> C["Como distribuir o trabalho?<br/>Balanceamento de carga"]
    C --> C1["Estático:<br/>blocos · cíclico · aleatório · grafos"]
    C --> C2["Dinâmico:<br/>Master/Slave · work stealing"]
    C1 & C2 --> D["Dados perto da computação:<br/>localidade"]
    D --> E["Implementação em Java"]
    E --> E1["Thread / Runnable"]
    E --> E2["ExecutorService + thread pool"]
    E --> E3["Callable + Future"]
    E --> E4["ForkJoin (→ Aula 08)"]
```

> **O que observar:** a aula sai do "*onde* está o paralelismo" (Aula 06) para o "**como executá-lo** de forma eficiente". No fim, os conceitos viram classes Java concretas.

---

## 1. De decomposição para threads

Retomando o exemplo da Aula 06: **1 milhão de registros**, calcular **média, mínimo e máximo**, com os dados divididos em 4 partições de 250 mil.

```mermaid
flowchart TB
    subgraph S1["Opção A: 1 thread por partição (4 threads)"]
        direction LR
        A1["Partição 1<br/>1 thread: média+mín+máx"]
        A2["Partição 2<br/>1 thread: média+mín+máx"]
        A3["Partição 3<br/>1 thread: média+mín+máx"]
        A4["Partição 4<br/>1 thread: média+mín+máx"]
    end
    subgraph S2["Opção B: 1 thread por tarefa × partição (12 threads)"]
        direction LR
        B1["Partição 1<br/>3 threads: média | mín | máx"]
        B2["Partição 2<br/>3 threads: média | mín | máx"]
        B3["Partição 3<br/>3 threads: média | mín | máx"]
        B4["Partição 4<br/>3 threads: média | mín | máx"]
    end
    S1 ~~~ S2
```

### Para discutir: qual é mais rápido?

**Resposta da aula: "teria que testar…, e depende da arquitetura do computador".** O raciocínio que importa:

| Opção A: 4 threads (1 por partição) | Opção B: 12 threads (1 por operação × partição) |
|---|---|
| Cada thread passa **uma única vez** pela partição e calcula as 3 operações juntas | Os mesmos dados são lidos **3 vezes**, uma por thread |
| Menos threads → menos criação, troca de contexto e sincronização | Mais paralelismo potencial, mas só ajuda se houver núcleos sobrando |
| **Provavelmente melhor aqui**, porque mín, máx e média são **baratas** e podem ser calculadas "juntas" | Pode ser melhor se as operações fossem **muito custosas** e independentes |

> 💡 **Lição:** "mais threads" não é sinônimo de "mais rápido". A decisão depende do **custo de cada operação**, de **quantas vezes os dados são percorridos** (memória e cache!) e do **hardware**.

---

## 2. Task, thread e chunk

| Termo | O que é | Analogia |
|---|---|---|
| **Task** (tarefa) | **Unidade de trabalho** que precisa ser executada | Um pedido na lanchonete |
| **Chunk** (pedaço) | **Porção de dados** processada como uma unidade de trabalho | Os ingredientes de um pedido |
| **Thread** | **Recurso de execução**: quem executa as tarefas | O cozinheiro |

⚠️ **Task e thread não são a mesma coisa.** Podemos (e geralmente devemos) ter **mais tasks do que threads**. Exemplo do slide: **4 cores, 4 threads, 32 chunks**. As threads vão pegando os chunks à medida que ficam livres.

```mermaid
flowchart LR
    subgraph FILA["Fila com 32 chunks"]
        direction TB
        Q1["chunk 1"] --- Q2["chunk 2"] --- Q3["..."] --- Q32["chunk 32"]
    end
    FILA --> T1["Thread 1 (core 1)"]
    FILA --> T2["Thread 2 (core 2)"]
    FILA --> T3["Thread 3 (core 3)"]
    FILA --> T4["Thread 4 (core 4)"]
```

> **O que observar:** é exatamente esse modelo que o `ExecutorService` implementa (seção 9): uma **fila de tarefas** consumida por um **conjunto fixo de threads**.

---

## 3. Granularidade, número de tasks e número de threads

> **[P2] Cai:** "explique o trade-off entre granularidade das tarefas e número de threads" (balanceamento × overhead). Ver o exemplo A/B/C discutido em aula nas [orientações](12b_orientacoes_prova2.md#24-exercício-granularidade--número-de-threads).

### 3.1 Granularidade

A **granularidade** determina o **tamanho das unidades de trabalho**.

| | Tasks **muito grandes** (granularidade grossa) | Tasks **muito pequenas** (granularidade fina) |
|---|---|---|
| Overhead de gerenciamento | ✅ Baixo | ❌ Alto: criação, escalonamento, sincronização |
| Balanceamento de carga | ❌ Poucas oportunidades | ✅ Muitas oportunidades |

**Trade-off:** granularidade × balanceamento × overhead. O objetivo é **trabalho suficiente para manter os processadores ocupados, sem overhead excessivo**.

### 3.2 Número de tasks

- **Não existe número universalmente ótimo.** É preciso ter tasks **suficientes para ocupar os processadores e permitir o balanceamento**. Por isso o nº de tasks/chunks costuma ser **maior** que o de threads.
- Tasks **em excesso** também aumentam o overhead.
- O tamanho ideal depende de: **custo de cada operação**, **quantidade de dados**, **variabilidade do custo** das tarefas, **número de processadores** e **overhead do mecanismo de execução**.

> O livro (*Patterns*, §4.6) dá uma regra prática para distribuição cíclica: algo como **10× mais tasks do que unidades de execução** costuma bastar para balancear estatisticamente.

### 3.3 Número de threads

| Tipo de aplicação | Ponto de partida | Por quê |
|---|---|---|
| **CPU-bound** (cálculo intenso) | ≈ número de **cores** | Mais threads que cores só geram **troca de contexto**; não há mais CPU para usar |
| **I/O-bound** (espera disco/rede) | **Mais** threads que cores | Enquanto uma thread espera I/O, outra usa a CPU, e o núcleo não fica parado |

Em Java, o número de núcleos lógicos sai de:

```java
int cores = Runtime.getRuntime().availableProcessors();   // na máquina de teste: 8
ExecutorService pool = Executors.newFixedThreadPool(cores);
```

- Deixe o valor **parametrizável**, para adaptar o programa a outras máquinas.
- Ajuste **experimentalmente** para encontrar a melhor configuração.

### 3.4 Threads não são gratuitas

- **Criar e gerenciar** uma thread tem custo; cada uma consome **memória** (pilha) e recursos do SO.
- **Troca de contexto** entre threads gera overhead. O livro *Distributed Systems* (§3.1, Nota 3.1) mostra que o custo **direto** da troca é pequeno (≈ 0,5–1 µs), mas o custo **indireto**, a "bagunça" que ela causa no **cache**, pode ser muito maior.
- **Sincronização** entre threads também custa.

➡️ **Não crie uma thread por unidade de trabalho.** Para processar milhões de elementos, use **poucas threads** e divida o trabalho entre elas.

### 3.5 Objetivo da programação paralela

> **O objetivo não é maximizar o número de threads.** É **maximizar o trabalho útil feito em paralelo**, mantendo os recursos ocupados e minimizando: ociosidade · sincronização · comunicação · troca de contexto · gerenciamento de tasks · outros overheads do paralelismo.

Na prática, a escolha de nº de threads e granularidade combina **análise da arquitetura + características do algoritmo + experimentação**.

---

## 4. Balanceamento de carga: estático × dinâmico

> **[P2] Cai:** estático (decidido antes da execução, em geral pelo programador) × dinâmico (durante a execução, conforme as threads terminam).

| | **Estático** | **Dinâmico** |
|---|---|---|
| Quando a divisão é feita | **Antes** de executar (compilação ou inicialização) | **Durante** a execução |
| Tamanho do bloco por thread | **Fixo** | Variável: as threads **pedem** trabalho ao terminar o anterior |
| Comunicação sobre o andamento | Nenhuma | Há coordenação (fila, mestre, roubo de trabalho) |

### 4.1 Estático

No exemplo de mín/máx/média, **pré-definimos** o tamanho da partição e cada partição recebe uma thread no próprio código. Não há comunicação entre as threads sobre o status do trabalho.

**Problema: carga heterogênea.** E se a Partição 1 tiver dados muito mais "pesados"?
- As threads 2, 3 e 4 terminam em milissegundos e **ficam ociosas** ("de braços cruzados");
- a thread 1 continua trabalhando sozinha por muito tempo;
- **no modelo estático, o tempo total é ditado pela thread mais lenta.**

| ✅ Vantagem | ❌ Desvantagem |
|---|---|
| **Custo de sincronização reduzido.** Ótimo quando o problema é bem conhecido ou embaraçosamente paralelo | Se as tarefas têm **custos diferentes**, gera **ociosidade** e atrasa o tempo total |

### 4.2 Dinâmico: exemplo do slide (8 tarefas, 4 threads)

O slide dá os custos **por thread** na divisão estática: T1 → P1+P2 = 20 ms, T2 → P3+P4 = 20 ms, T3 → P5+P6 = 100 ms, T4 → P7+P8 = 100 ms.
**Suposição para calcular** (o slide não dá o custo individual): P1…P4 = **10 ms** cada e P5…P8 = **50 ms** cada.

```mermaid
gantt
    title ESTÁTICO: cada thread recebe 2 tarefas fixas (total = 100 ms)
    dateFormat x
    axisFormat %L
    section Thread 1
    P1 :0, 10
    P2 :10, 20
    ociosa :crit, 20, 100
    section Thread 2
    P3 :0, 10
    P4 :10, 20
    ociosa :crit, 20, 100
    section Thread 3
    P5 :0, 50
    P6 :50, 100
    section Thread 4
    P7 :0, 50
    P8 :50, 100
```

```mermaid
gantt
    title DINÂMICO: quem termina pega a próxima tarefa da fila (total = 60 ms)
    dateFormat x
    axisFormat %L
    section Thread 1
    P1 :0, 10
    P5 :10, 60
    section Thread 2
    P2 :0, 10
    P6 :10, 60
    section Thread 3
    P3 :0, 10
    P7 :10, 60
    section Thread 4
    P4 :0, 10
    P8 :10, 60
```

> **O que observar:** no estático, T1 e T2 ficam **80 ms ociosas** (vermelho) e o total é 100 ms. No dinâmico, a distribuição **se adapta ao andamento**: cada thread pega P5…P8 assim que termina sua primeira tarefa, e o total cai para **60 ms**. A ordem P1→P5, P2→P6… é exatamente a do slide.

| ✅ Vantagens do dinâmico | ❌ Desvantagens do dinâmico |
|---|---|
| Melhor aproveitamento dos recursos | Custo de **gerenciamento** das tarefas |
| Adapta-se a **custos diferentes** | Custo de **comunicação/coordenação** |
| | Tasks **muito pequenas** geram overhead |

### 4.3 Qual escolher?

| Situação | Escolha |
|---|---|
| Custo de cada tarefa **conhecido e aproximadamente uniforme** | **Estático** |
| Custo **não uniforme**, imprevisível ou problema **recursivo** | **Dinâmico** |

O livro (*Patterns*, §4.4) reforça: **escalonamentos estáticos têm o menor overhead e devem ser usados sempre que possível**. O dinâmico entra quando o custo das tarefas (ou a capacidade das máquinas) **varia muito e de forma imprevisível**.

---

## 5. Mapeamento estático: como dividir os dados

> **[P2]** Possível pergunta conceitual citada por ela: "diferença entre decomposição em blocos e decomposição geométrica" (blocos = forma de distribuir os dados; geométrica = padrão de estrutura, Aula 06).

Problemas com balanceamento estático costumam usar **decomposição de dados**. Mapear uma parte dos dados para um processo equivale a mapear uma tarefa para esse processo. As formas mais comuns de representar dados são **vetores (arrays)** e **grafos**.

### 5.1 As distribuições em um vetor de 16 posições e 4 threads

Saída real de um programa que imprime **qual thread fica com cada índice**:

| Índice → | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Bloco** `i / (n/p)` | 0 | 0 | 0 | 0 | 1 | 1 | 1 | 1 | 2 | 2 | 2 | 2 | 3 | 3 | 3 | 3 |
| **Cíclico** `i % p` | 0 | 1 | 2 | 3 | 0 | 1 | 2 | 3 | 0 | 1 | 2 | 3 | 0 | 1 | 2 | 3 |
| **Bloco-cíclico** (blocos de 2) `(i/2) % p` | 0 | 0 | 1 | 1 | 2 | 2 | 3 | 3 | 0 | 0 | 1 | 1 | 2 | 2 | 3 | 3 |

```java
int dono_bloco         = i / (n / p);      // entradas contínuas
int dono_ciclico       = i % p;            // round robin elemento a elemento
int dono_bloco_ciclico = (i / b) % p;      // blocos de tamanho b distribuídos em round robin
```

### 5.2 Distribuição em blocos

- Particiona o vetor em **blocos de entradas contínuas**, um por processo.
- Útil quando a computação precisa interagir com elementos **"perto"** (vizinhos), e tem **ótima localidade** (seção 7).
- **Matriz n × n:**
  - **1 dimensão:** cada processo recebe uma **faixa de linhas** (ou de colunas);
  - **múltiplas dimensões (2D):** cada processo recebe um **sub-bloco retangular**. Os blocos ficam mais "quadrados", e há menos fronteira para trocar (Aula 06, §4.3.1).

| Matriz 4×4, 4 processos: blocos 1D (por linhas) | Matriz 4×4, 4 processos: blocos 2D |
|---|---|
| `P0 P0 P0 P0`<br/>`P1 P1 P1 P1`<br/>`P2 P2 P2 P2`<br/>`P3 P3 P3 P3` | `P0 P0 P1 P1`<br/>`P0 P0 P1 P1`<br/>`P2 P2 P3 P3`<br/>`P2 P2 P3 P3` |

### 5.3 Blocos cíclicos

- Particiona o vetor em **muito mais blocos do que processos**;
- distribui os blocos em **round robin** (de forma rotativa), e cada processo recebe **vários blocos não adjacentes**;
- assim, **cada processo recebe uma amostra de todas as partes do vetor**, o que **balanceia** a carga.

> 💡 **Quando ajuda:** se o custo **varia ao longo do vetor**. O livro (*Patterns*, §3.3) cita a eliminação em uma matriz, que limpa as colunas da esquerda para a direita. Com distribuição em blocos de colunas, quem tem as primeiras colunas termina cedo e fica ocioso. Com a cíclica, todos ficam com partes "caras" e "baratas". A analogia do livro é **distribuir cartas de um baralho**.

### 5.4 Blocos aleatórios

- A distribuição cíclica pode falhar se o vetor tiver **padrões especiais**. Exemplo: se os elementos caros estão justamente nas posições múltiplas de `p`, todos caem no mesmo processo.
- Solução: muitos mais blocos que processos (como antes), mas com **elementos aleatórios** em cada bloco, em vez de adjacentes.
- Com valores aleatórios, o balanceamento fica **mais uniforme dentro de cada bloco**.

### 5.5 Particionamento de grafos

- Para algoritmos sobre **estruturas esparsas e interação irregular**: malhas (*mesh*), dados espaciais, simulação de eventos.
- Mesmo que cada ponto tenha custo parecido, o ideal é que **pontos próximos fiquem na mesma partição**.
- Particionar **por região**, de modo que: (1) cada partição tenha **aproximadamente o mesmo número de pontos** (balanceamento); (2) as arestas **entre partições** sejam **mínimas** (pouca comunicação).

### 5.6 Outros tipos de mapeamento

- **Por tarefa:** usar o **grafo de dependências** (Aula 06) para identificar as tarefas independentes e distribuí-las.
- **Hierárquico:** mapear tarefas em **subtarefas** de acordo com a necessidade de execução (ex.: tarefas grandes divididas em níveis).

---

## 6. Mapeamento dinâmico e load balancer

O mapeamento dinâmico entra quando (1) o mapeamento estático fica **muito desbalanceado**, ou (2) as tarefas surgem **dinamicamente**, ou seja, o **grafo de dependências é dinâmico**. Ele pode ser **centralizado** ou **distribuído**.

### 6.1 Centralizado: Master/Slave

- Todas as tarefas executáveis ficam numa **estrutura de dados central** (ex.: uma lista ou fila) ou são mantidas por um **processo especial**, o **mestre** (*master*).
- Os processos que dependem do mestre para obter trabalho são os **escravos** (*slaves*), também chamados *workers*.
- Uma nova tarefa criada durante a execução é **adicionada à estrutura** ou **reportada ao mestre**.

```mermaid
sequenceDiagram
    participant M as Master (fila de tarefas)
    participant W1 as Worker 1
    participant W2 as Worker 2
    M->>W1: tarefa P1
    M->>W2: tarefa P2
    W1-->>M: resultado P1 + "quero mais"
    M->>W1: tarefa P3
    W2-->>M: resultado P2 + "quero mais"
    M->>W2: tarefa P4
    Note over M: Todo pedido passa pelo mestre:<br/>com muitos workers, ele vira gargalo
```

| ✅ Vantagem | ❌ Desvantagem |
|---|---|
| Em geral **fácil de implementar**; balanceia **automaticamente** (quem termina antes pega mais) | **Limite de escalabilidade**: o acesso ao mestre/estrutura central vira **gargalo** (*bottleneck*) |

O livro (*Patterns*, §5.5) acrescenta: Master/Worker escala bem **enquanto o nº de tarefas for muito maior que o de workers**. Ele observa ainda que o `ThreadPoolExecutor` do Java **implementa o padrão Master/Worker** com um pool fixo de threads. É o `newFixedThreadPool` da seção 9.

### 6.2 Distribuído: roubo de trabalho (*work stealing*)

> **[P2]** Perguntado em aula: *work stealing* é balanceamento **dinâmico** e está implementado no Fork/Join do Java.

Para fugir do gargalo central, **cada unidade tem sua própria fila**. Quando a fila esvazia, ela **rouba** tarefas da fila de outra (escolhida aleatoriamente). Segundo o livro, o roubo é feito do **final** da fila da vítima, onde estão as tarefas **maiores** (criadas primeiro), enquanto as pequenas ficam com quem as criou. É o algoritmo do **`ForkJoinPool`** (seção 11 e Aula 08).

### 6.3 Load balancer em sistemas distribuídos

Atenção, é **outro conceito com o mesmo nome**:
- Numa aplicação **cliente-servidor**, o *load balancer* é um componente do **middleware** que distribui as **conexões ativas** entre vários nós (servidores) do sistema distribuído.
- O livro *Distributed Systems* (§3.4.4) o chama de **switch / front end** do cluster: um **único ponto de acesso** que esconde do cliente a existência de vários servidores.

```mermaid
flowchart LR
    C1["Cliente"] & C2["Cliente"] & C3["Cliente"] --> LB["Load balancer<br/>(front end)"]
    LB --> S1["Servidor 1"]
    LB --> S2["Servidor 2"]
    LB --> S3["Servidor 3"]
```

| Algoritmo | Tipo | Como funciona |
|---|---|---|
| **Round Robin** | **Estático** | Distribui as conexões entre os servidores numa **ordem fixa e sequencial** (1, 2, 3, 1, 2, 3…). O livro o chama de "a política mais simples" |
| **Least Connection** | **Dinâmico** | Envia cada nova requisição ao servidor com o **menor número de conexões ativas** no momento |

> **Paralelo com a programação paralela:** round robin ↔ distribuição **cíclica** (estática); least connection ↔ **fila dinâmica** (quem está mais livre recebe).

---

## 7. Localidade de dados (*data locality*)

Uma boa distribuição do trabalho **não basta**. Também é preciso garantir que os **dados de cada thread/processo estejam perto da computação**.

- **Data locality** = proximidade entre o processamento e os dados de que ele precisa.
- **Ideia:** trazer a computação para perto dos dados. Quanto mais perto, menor o custo de acesso.
- Dados **próximos**: acesso rápido e melhor uso das **caches**. Dados **distantes**: acesso caro e mais tráfego.
- **Objetivo:** evitar movimentação e acesso caro aos dados.

```mermaid
flowchart LR
    R["Registradores<br/>~1 ciclo"] --> L1["Cache L1"] --> L2["Cache L2"] --> L3["Cache L3"] --> RAM["Memória RAM"] --> D["Disco / SSD"] --> N["Outro nó<br/>(rede)"]
```

> **O que observar:** da esquerda para a direita, cada nível é **maior e mais lento**. Um bom programa paralelo faz a maior parte dos acessos nos níveis da esquerda.

| Tipo | Definição | Exemplo |
|---|---|---|
| **Localidade espacial** | Se acessamos um dado, é provável que **dados próximos** também sejam acessados | Percorrer um vetor em ordem: a cache traz uma "linha" inteira de elementos vizinhos |
| **Localidade temporal** | Se acessamos um dado, é provável que ele seja acessado **de novo em breve** | Uma variável acumuladora (`soma`) usada a cada iteração |

**No exemplo de mín/máx/média** com mapeamento estático em blocos, cada thread trabalha numa **região contínua**. Ela não acessa a partição de outra, então há menos acessos desnecessários e boa localidade espacial.

> **Na Aula 08:** o Hadoop leva essa ideia para o cluster. Em vez de mover terabytes pela rede até o programa, **leva o código até o nó onde o dado está**.

**Para ir além: medindo a localidade em Java.** Em Java, uma matriz `int[][]` é um vetor de linhas, e os elementos de **uma linha** ficam contíguos na memória:

```java
for (int i = 0; i < n; i++)          // por LINHA: acesso sequencial (localidade espacial)
    for (int j = 0; j < n; j++) s += m[i][j];

for (int j = 0; j < n; j++)          // por COLUNA: salta de linha em linha
    for (int i = 0; i < n; i++) s += m[i][j];
```

Medido na máquina de teste (matriz 4000 × 4000, JDK 24): **por linha ≈ 3–14 ms · por coluna ≈ 187 ms**, para o **mesmo resultado**. Os números exatos variam por máquina; a diferença de uma ordem de grandeza é o que importa.

---

## 8. Java: `Thread` e `Runnable`

Uma `Thread` em Java é, por padrão, uma **thread do sistema operacional** (a documentação chama de *platform thread*). A tarefa é descrita por um `Runnable`, cujo método `run()` **não retorna nada**.

**Cenário do slide:** "implementei uma task com `Runnable` e quero rodá-la 1000 vezes".

```java
Runnable tarefa = () -> { /* trabalho */ };
Thread[] ts = new Thread[1000];
for (int i = 0; i < 1000; i++) {
    ts[i] = new Thread(tarefa);   // 1000 threads do SO!
    ts[i].start();                // start() cria a thread e chama run() nela
}
for (Thread t : ts) t.join();     // join() espera cada uma terminar
```

**Por que começar 1000 threads não é a melhor escolha:**
- a arquitetura tem um número **limitado de cores** (ex.: 8): as outras 992 só disputam CPU com **troca de contexto**;
- **criar, manter e sincronizar** threads é difícil e custoso.

**O ideal:** limitar o número de threads e ainda assim executar as 1000 tasks. Para isso, usamos uma **fila com as 1000 tarefas** e um **grupo pequeno de threads** que pega a próxima tarefa ao terminar a anterior. ➡️ É exatamente o que o **`ExecutorService`** oferece.

> ⚠️ **`start()` × `run()`:** `t.start()` cria a nova thread e executa `run()` **nela**. Chamar `t.run()` diretamente executa o código **na thread atual**, sem paralelismo nenhum. É uma pegadinha clássica de "verifique se o código está correto".

---

## 9. Java: `ExecutorService`

> **[P2] Cai como interpretação:** quantas tarefas, quantas threads e qual decomposição. Atenção: `newFixedThreadPool(2)` = 2 threads no pool, **além da `main`**; se a pergunta não for explícita, diga as duas coisas. Modelo: [Aula 12, ex. 4](12_joins_spark_revisao_prova.md#exercício-4--programa-java-rastreiopool).

O `ExecutorService` fornece a **infraestrutura para executar e gerenciar tasks**:
- **o programador define *o que* fazer:** as tasks (`Runnable` ou `Callable`);
- **o `ExecutorService` gerencia *como* executar:** as threads, a fila e o mapeamento task → thread.

```mermaid
flowchart LR
    subgraph PROG["Programador"]
        T1["Task 1"] & T2["Task 2"] & T3["Task 3"] & TN["... Task N"]
    end
    subgraph EX["ExecutorService"]
        Q["Task Queue<br/>(fila)"]
        subgraph POOL["Thread Pool (ex.: 3 threads)"]
            W1["pool-1-thread-1"]
            W2["pool-1-thread-2"]
            W3["pool-1-thread-3"]
        end
        Q --> W1 & W2 & W3
    end
    T1 & T2 & T3 & TN -->|submit| Q
```

**Thread pool:** em vez de criar uma thread por tarefa, o `ExecutorService` mantém um **conjunto de threads reutilizáveis**. Quando uma tarefa termina, ele **mapeia outra tarefa para aquela thread**. Isso é **balanceamento dinâmico** do tipo Master/Worker (seção 6.1).

### 9.1 Tipos de thread pool (`Executors`)

| Fábrica | Comportamento (Javadoc) | Use quando… |
|---|---|---|
| `newFixedThreadPool(n)` | **n threads fixas e reutilizáveis** sobre uma fila **ilimitada**; se todas estão ocupadas, as novas tarefas **esperam na fila** | **Padrão para CPU-bound**: `n ≈ availableProcessors()` |
| `newCachedThreadPool()` | **Cria threads conforme a demanda** e reutiliza as livres; **sem limite fixo**; threads ociosas por **60 segundos** são removidas | Muitas tarefas **curtas e assíncronas** (ex.: I/O). ⚠️ Com tarefas longas, pode criar threads demais |
| `newSingleThreadExecutor()` | **Uma única thread**; tarefas executadas **estritamente em ordem** (FIFO), uma de cada vez | Quando a **ordem** importa ou o recurso não pode ser acessado em paralelo |
| `newScheduledThreadPool(n)` | Executa tarefas **após um atraso** (*delay*) ou **periodicamente** | Agendamentos: "a cada 5 s, verifique…" |

### 9.2 O código da professora: `Runnable` + `FixedThreadPool`

```java
class TaskRunnable implements Runnable {
    @Override
    public void run() {
        // Exibe o nome da thread atual para ver as threads do pool em ação
        String nomeThread = Thread.currentThread().getName();
        System.out.println("Executando na: " + nomeThread);
    }
}

public class Main {
    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(3); // 1. pool com 3 threads
        for (int i = 0; i < 6; i++) {
            executor.submit(new TaskRunnable());                     // 2. 6 tarefas para a fila
        }
        executor.shutdown();                                         // 3. encerra o executor
    }
}
```

**Interpretação linha a linha:**

| Linha | O que acontece |
|---|---|
| `newFixedThreadPool(3)` | Cria um pool com **3 threads** (`pool-1-thread-1`, `-2`, `-3`) |
| `submit(new TaskRunnable())` × 6 | Coloca **6 tarefas na fila**. O `submit` **não espera**: retorna na hora |
| `Thread.currentThread().getName()` | Dentro de `run()`, devolve o nome da thread **do pool** que executa a tarefa |
| `shutdown()` | **Não aceita novas tarefas**, mas **deixa terminar** as que já estão na fila |

**Saída real (uma execução):**
```
Executando na: pool-1-thread-2
Executando na: pool-1-thread-1
Executando na: pool-1-thread-3
Executando na: pool-1-thread-2
Executando na: pool-1-thread-1
Executando na: pool-1-thread-2
```

> **O que observar:** (1) só aparecem **3 nomes diferentes** para **6 tarefas**: as threads são **reutilizadas**. (2) A **ordem não é determinística**: rode de novo e ela muda. (3) Uma thread pode executar mais tarefas que outra (aqui a `thread-2` fez 3). É o balanceamento dinâmico em ação.

### 9.3 Encerrando o executor: `shutdown`, `shutdownNow`, `awaitTermination`

| Método | O que faz | Bloqueia? |
|---|---|---|
| `shutdown()` | Para de aceitar tarefas novas; as **já submetidas são executadas** | **Não**: retorna na hora |
| `shutdownNow()` | **Tenta** interromper as tarefas em execução e **descarta** as da fila (devolve a lista delas) | **Não** |
| `awaitTermination(t, unidade)` | **Espera** até todas terminarem após o shutdown (ou até o timeout) | **Sim** |

> ⚠️ **Esquecer o `shutdown()` faz o programa não terminar.** As threads de um `FixedThreadPool` ficam vivas esperando novas tarefas e impedem a JVM de encerrar. Testado: o programa imprimiu o resultado e **ficou travado** até ser morto pelo `timeout`.

---

## 10. Java: `Callable` e `Future`

### 10.1 O problema do `Runnable`: sem retorno

`run()` é `void`. Nos exercícios anteriores da disciplina, foi preciso criar **variáveis/atributos** para guardar o resultado e um *getter* para lê-lo depois (slide p.51):

```java
public class BuscaVetorMultiThread implements Runnable {
    private Integer posicao;                     // ← resultado guardado num atributo
    // ... construtor recebe vetor, inicio, fim, input
    public Integer getPosicao() { return posicao; }
    @Override
    public void run() {
        for (int i = inicio; i < fim; i++) {
            if (vetor[i] == input) { posicao = i; break; }
        }
    }
}
```

Seria muito mais fácil **retornar o valor no próprio método**. ➡️ `Callable`.

### 10.2 `Callable<V>`

- **Interface funcional** que representa uma tarefa executada de forma **assíncrona**.
- Método `V call() throws Exception`: **retorna um valor** de qualquer tipo `V` e **pode lançar exceções**.
- Ela só define a tarefa (a **"receita"**). Precisa de um **`ExecutorService`** para ser executada.
- **Não pode ser usada com a classe `Thread`** (`new Thread(callable)` **não compila**).

| | `Runnable` | `Callable<V>` |
|---|---|---|
| Método | `void run()` | `V call() throws Exception` |
| Retorna valor? | ❌ | ✅ Tipo `V` |
| Lança exceção verificada? | ❌ | ✅ |
| `new Thread(...)` aceita? | ✅ | ❌ Erro de compilação |
| `executor.submit(...)` aceita? | ✅ Devolve `Future<?>` | ✅ Devolve `Future<V>` |
| `executor.execute(...)` aceita? | ✅ | ❌ (só `Runnable`) |

Erro real ao tentar `new Thread(callable)`:
```
error: no suitable constructor found for Thread(Callable<Integer>)
    constructor Thread.Thread(Runnable) is not applicable
      (argument mismatch; Callable<Integer> cannot be converted to Runnable)
```

### 10.3 `Future<V>`: o "ticket de espera"

**`Future`** guarda a resposta de uma `Callable`. Ele representa o **resultado de uma tarefa que ainda está em processamento**.

**Analogia da lanchonete (slide):**

```mermaid
sequenceDiagram
    participant Voce as Você (thread main)
    participant Caixa as Caixa (ExecutorService)
    participant Cozinha as Cozinha (thread do pool)
    Voce->>Caixa: entrega a receita (submit(Callable))
    Caixa-->>Voce: devolve a senha (Future) na hora
    Caixa->>Cozinha: prepara em segundo plano (call())
    Note over Voce: você continua fazendo outras coisas
    Voce->>Caixa: "meu lanche?" (future.get())
    Note over Voce: get() BLOQUEIA até ficar pronto
    Cozinha-->>Voce: lanche pronto (valor de retorno)
```

**Do envio ao resultado:**
1. **Envio assíncrono:** `executor.submit(callable)` devolve o `Future` **instantaneamente** e o programa **continua sem travar**.
2. **Busca do resultado:** `future.get()` faz a thread atual **pausar e esperar** até a `Callable` terminar e devolver o valor.

| Método do `Future` | O que faz | Bloqueia? |
|---|---|---|
| `get()` | Espera o término e devolve o resultado | **Sim**, indefinidamente |
| `get(tempo, unidade)` | Espera **no máximo** o tempo dado; se estourar, lança `TimeoutException` | Sim, com limite |
| `isDone()` | `true` se terminou (normalmente, com exceção ou cancelada) | Não |
| `cancel(mayInterrupt)` | **Tenta** cancelar; se a tarefa não começou, não executará mais | Não |
| `isCancelled()` | `true` se foi cancelada antes de terminar | Não |

**Exceções de `get()`:** `ExecutionException` (a `call()` lançou uma exceção, que vem **embrulhada**), `InterruptedException`, `CancellationException` e, no `get` com tempo, `TimeoutException`.

**Boa prática do slide:** use `future.get(tempo, unidade)` com limite de tempo, para o sistema **não travar para sempre** se a tarefa falhar. Saída real de um teste (tarefa de 3 s, timeout de 1 s):

```java
Future<String> f = ex.submit(() -> { Thread.sleep(3000); return "pronto"; });
System.out.println("isDone logo após submit? " + f.isDone());      // false
try {
    System.out.println(f.get(1, TimeUnit.SECONDS));
} catch (TimeoutException e) {
    System.out.println("Timeout! cancel=" + f.cancel(true) + " isCancelled=" + f.isCancelled());
}
```
```
isDone logo após submit? false
Timeout! cancelando... cancel=true isCancelled=true
```

### 10.4 O código da professora: `Callable` + `List<Future>`

```java
class TaskCallable implements Callable<Integer> {
    @Override
    public Integer call() throws Exception {
        return RandomGenerator.getDefault().nextInt();   // Java 17+
    }
}

// no main:
ExecutorService executor = Executors.newFixedThreadPool(3);
ArrayList<Future<Integer>> future = new ArrayList<>();
for (int i = 0; i < 6; i++) {                                  // LAÇO 1: só submete
    Future<Integer> callable_return = executor.submit(new TaskCallable());
    future.add(callable_return);
}
// outras coisas (a main pode trabalhar enquanto as tarefas rodam)
for (int i = 0; i < future.size(); i++) {                      // LAÇO 2: espera e lê
    System.out.println("Valor do future" + future.get(i).get());
}
executor.shutdown();
```

**Por que dois laços?**
- **Laço 1** só **submete** as 6 tarefas e guarda os 6 "tickets" (`Future`). Como `submit` não bloqueia, as 6 tarefas entram na fila quase ao mesmo tempo e as 3 threads começam a trabalhar **em paralelo**.
- **Laço 2** chama `get()` em cada `Future`, na **ordem da lista**. Se a tarefa 0 ainda não terminou, a main espera por ela, mas as outras continuam rodando no pool. Os valores saem na **ordem de submissão**, não na ordem de término.
- Se o `get()` estivesse **dentro do laço 1**, cada tarefa seria submetida só depois da anterior terminar, e o programa ficaria **sequencial**. Ver Q3 da seção 13 (medido: 2016 ms × 501 ms).

> `future.get(i).get()`: o primeiro `get(i)` é do **`ArrayList`** (pega o i-ésimo `Future`); o segundo `.get()` é do **`Future`** (espera o resultado). São dois métodos diferentes com o mesmo nome.

### 10.5 Quadro "onde usar"

| Situação | Use | Não use |
|---|---|---|
| Executar N tarefas com poucas threads | `ExecutorService` + `newFixedThreadPool` | Criar N objetos `Thread` |
| Tarefa sem retorno | `Runnable` (`execute` ou `submit`) | — |
| Tarefa **com retorno** ou que lança exceção verificada | `Callable<V>` + `submit` → `Future<V>` | `Runnable` + atributo + getter (funciona, mas é trabalhoso) |
| Disparar sem esperar resultado | `execute(runnable)` | — |
| Precisa do resultado ou de saber se terminou | `submit(...)` → `Future` | `execute` (não devolve nada) |
| Submeter uma coleção de `Callable`s e esperar todas | `invokeAll(lista)` → `List<Future<V>>` já concluídos | — |
| Esperar o resultado sem risco de travar para sempre | `get(tempo, TimeUnit)` | `get()` sem limite em tarefas que podem falhar |
| Terminar o programa | `shutdown()` (+ `awaitTermination` se precisar esperar) | Esquecer de encerrar |

---

## 11. Java: introdução ao `ForkJoin`

> Aprofundado na **[Aula 08](08_forkjoin_bigdata_hadoop.md)**. Aqui fica a ideia geral.

- **`ForkJoinPool`:** framework para acelerar tarefas pesadas que podem ser **quebradas em subtarefas menores** executadas em paralelo. É o padrão **Divide and Conquer** da Aula 06.
- **Fork (dividir):** quebra a tarefa grande em menores, **recursivamente**, até ficarem simples o suficiente (caso base).
- **Join (juntar):** aguarda cada subtarefa e **combina** os resultados.
- **Diferencial: *work stealing*.** Se a thread A termina antes da B, A **"rouba" trabalho do final da fila** da B. É o mapeamento **dinâmico distribuído** da seção 6.2.
- **Classes:** `ForkJoinPool` (gerenciador), `RecursiveTask<V>` (tarefa **com** retorno) e `RecursiveAction` (tarefa **sem** retorno).

| | `ExecutorService` (pool fixo) | `ForkJoinPool` |
|---|---|---|
| Tarefas típicas | Independentes, "planas" (lista de tarefas) | **Recursivas**, que geram subtarefas |
| Balanceamento | Fila central compartilhada (Master/Worker) | Uma fila por thread + **roubo de trabalho** |
| Padrão (Aula 06) | Task Parallelism | Divide and Conquer / Recursive Data |

---

## 12. Exercício 1 resolvido (para leitura)

> **Enunciado (slide p.58):** dado o vetor `[1,70,40,20,89,34,71,56,2,6,30,23]`, escreva um programa que use **3 threads** (além da main) para calcular a **soma** dos valores. Use a interface `Callable`.

```java
class SomaParcial implements Callable<Integer> {
    private final int[] vetor;
    private final int inicio, fim;                  // intervalo [inicio, fim)

    SomaParcial(int[] vetor, int inicio, int fim) {
        this.vetor = vetor; this.inicio = inicio; this.fim = fim;
    }

    @Override
    public Integer call() {                          // cada tarefa devolve SUA soma parcial
        int soma = 0;
        for (int i = inicio; i < fim; i++) soma += vetor[i];
        return soma;
    }
}

public class SomaCallable {
    public static void main(String[] args) throws Exception {
        int[] vetor = {1, 70, 40, 20, 89, 34, 71, 56, 2, 6, 30, 23};
        int numThreads = 3;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Integer>> futuros = new ArrayList<>();

        int tamanho = vetor.length / numThreads;                         // 12 / 3 = 4
        for (int t = 0; t < numThreads; t++) {
            int inicio = t * tamanho;
            int fim = (t == numThreads - 1) ? vetor.length : inicio + tamanho;
            futuros.add(executor.submit(new SomaParcial(vetor, inicio, fim)));
        }

        int total = 0;
        for (Future<Integer> f : futuros) total += f.get();              // redução
        executor.shutdown();
        System.out.println("Soma total = " + total);
    }
}
```

**Saída real** (com um `println` extra dentro de `call()` para ver as threads):
```
pool-1-thread-2 somou [4,8) = 250
pool-1-thread-3 somou [8,12) = 61
pool-1-thread-1 somou [0,4) = 131
Soma total = 442
```

**Pontos de leitura:**

| Pergunta | Resposta |
|---|---|
| Qual decomposição? | **De dados**, em **blocos** contínuos de 4 elementos (mapeamento **estático**) |
| Por que `int[]` compartilhado não dá problema? | O vetor é **somente leitura** e cada tarefa lê só a sua faixa (*effectively-local*). Ninguém escreve nele |
| Onde está a "dependência separável"? | A soma total é uma **redução**: cada `call()` acumula numa variável **local** e a main combina os parciais |
| Para que serve o `(t == numThreads - 1) ? vetor.length : ...`? | Garante que o último bloco pegue o **resto** se o tamanho não for divisível (Aula 06, §6.1). Aqui 12/3 é exato, mas o código fica correto para qualquer tamanho |
| Por que as linhas saem fora de ordem e o total está certo? | As threads terminam em ordem imprevisível, mas a main faz `get()` na ordem da lista e **soma tudo**; a ordem da soma não importa |
| Quantas threads existem? | 3 do pool **+ a main** (que só submete e combina) |

---

## 13. Treino de leitura de código

> Todos os comportamentos descritos nos gabaritos foram **testados** (JDK 24).

**Q1 — Interprete.** Quantas linhas o programa imprime, quantos nomes de thread **diferentes** aparecem no máximo e a ordem é sempre a mesma?

```java
ExecutorService ex = Executors.newFixedThreadPool(2);
for (int i = 0; i < 5; i++)
    ex.submit(() -> System.out.println(Thread.currentThread().getName()));
ex.shutdown();
```

<details><summary><b>Gabarito Q1</b></summary>

**5 linhas** (uma por tarefa), com **no máximo 2 nomes distintos** (`pool-1-thread-1` e `pool-1-thread-2`), porque o pool tem 2 threads reutilizadas. A ordem **não** é determinística, e uma thread pode executar mais tarefas que a outra.
</details>

**Q2 — Está correto?**

```java
Callable<Integer> tarefa = () -> 42;
Thread t = new Thread(tarefa);
t.start();
```

<details><summary><b>Gabarito Q2</b></summary>

**Não compila.** `Thread` só aceita `Runnable`, e um `Callable` não pode ser usado com a classe `Thread` (slide p.52). Erro: `no suitable constructor found for Thread(Callable<Integer>)`.

**Correção:** `ExecutorService ex = Executors.newSingleThreadExecutor(); Future<Integer> f = ex.submit(tarefa); System.out.println(f.get()); ex.shutdown();`
</details>

**Q3 — Está correto? Qual o problema de desempenho?** Cada tarefa leva 500 ms e o pool tem 4 threads.

```java
int soma = 0;
for (int i = 0; i < 4; i++) {
    Future<Integer> f = ex.submit(lenta(i));
    soma += f.get();
}
```

<details><summary><b>Gabarito Q3</b></summary>

O resultado está **certo**, mas o código está **sequencial**: o `get()` bloqueia **antes** de submeter a próxima tarefa, então só uma roda por vez. Medido: **≈ 2016 ms**.

**Correção:** submeter todas primeiro, guardar os `Future`s numa lista e só depois chamar `get()` (o padrão do `Main.java` da professora). Medido: **≈ 501 ms**, as 4 em paralelo.
</details>

**Q4 — O que acontece ao executar?**

```java
public static void main(String[] args) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Integer> f = executor.submit(() -> 10 + 20);
    System.out.println("Resultado: " + f.get());
}
```

<details><summary><b>Gabarito Q4</b></summary>

Imprime `Resultado: 30` e **o programa não termina**. Faltou `executor.shutdown()`: as threads do pool ficam vivas esperando tarefas e mantêm a JVM rodando. Testado: só parou quando foi morto por `timeout`.
</details>

**Q5 — Está correto?** 4 threads incrementam uma variável compartilhada.

```java
static long soma = 0;
// ...
for (int t = 0; t < 4; t++)
    ex.submit(() -> { for (int i = 0; i < 1_000_000; i++) soma++; });
ex.shutdown();
ex.awaitTermination(1, TimeUnit.MINUTES);
System.out.println(soma);       // esperado: 4000000
```

<details><summary><b>Gabarito Q5</b></summary>

**Incorreto: condição de corrida** (*race condition*). `soma++` é **ler → somar → escrever**. Duas threads leem o mesmo valor e uma sobrescreve o incremento da outra. Saídas reais em duas execuções: **2831936** e **1071692**, nunca 4000000, e diferentes a cada execução.

Na classificação da Aula 06, `soma` é um dado **read-write compartilhado**, do tipo **acumulação**. **Correção sem locks:** cada tarefa vira um `Callable<Long>` que soma numa variável **local** e devolve; a main combina os `Future`s (**redução**), exatamente como no Exercício 1.
</details>

**Q6 — Escolha o pool.** Qual fábrica de `Executors` usar em cada caso?
(a) Somar um vetor enorme numa máquina de 8 núcleos. (b) Um servidor que recebe muitas requisições curtas que esperam o banco de dados. (c) Gravar entradas num log que precisa ficar na ordem exata. (d) Fazer backup a cada 10 minutos.

<details><summary><b>Gabarito Q6</b></summary>

(a) `newFixedThreadPool(8)`, pois é CPU-bound (nº de threads ≈ nº de cores). (b) `newCachedThreadPool()` ou um fixo **maior que o nº de cores**, porque é I/O-bound com tarefas curtas. (c) `newSingleThreadExecutor()`, que executa em ordem FIFO, uma de cada vez. (d) `newScheduledThreadPool(1)`, para execução periódica.
</details>

**Q7 — Está correto?** Cada thread deveria executar uma tarefa em paralelo.

```java
Thread t1 = new Thread(tarefaA);
Thread t2 = new Thread(tarefaB);
t1.run();
t2.run();
```

<details><summary><b>Gabarito Q7</b></summary>

**Sem paralelismo.** `run()` chamado diretamente executa o código **na thread atual** (a main), uma tarefa depois da outra. O correto é `t1.start(); t2.start();` (e `join()` se for preciso esperar).
</details>

**Q8 — Balanceamento.** Um vetor tem custo de processamento crescente com o índice (o elemento `i` custa `i` unidades). Com 4 threads, qual distribuição estática balanceia melhor: blocos ou cíclica? Por quê?

<details><summary><b>Gabarito Q8</b></summary>

**Cíclica** (ou bloco-cíclica). Em blocos, a última thread fica com todos os elementos mais caros e define o tempo total. Na cíclica, cada thread recebe elementos de **todas as regiões** do vetor (baratos e caros), e as cargas ficam parecidas. É a ideia de "distribuir cartas" do livro.
</details>

---

## 14. Erros comuns / o que cai em prova

1. **Confundir task com thread.** Task = trabalho; thread = quem executa. Use **mais tasks que threads**.
2. **"Mais threads = mais rápido".** Falso: depende do custo das operações, de quantas vezes os dados são percorridos e do hardware (discussão 4 × 12 threads).
3. **CPU-bound com threads ≫ cores.** Só gera troca de contexto. I/O-bound é a exceção.
4. **Estático com carga heterogênea.** O tempo total = a thread mais lenta. Use dinâmico (fila) ou cíclico.
5. **Master/Slave escala até certo ponto.** O mestre (estrutura central) vira gargalo.
6. **Load balancer de SD ≠ balanceamento de carga em programação paralela.** O primeiro distribui **conexões** entre **servidores**.
7. **`new Thread(callable)`** não compila.
8. **`get()` dentro do laço de `submit`** deixa tudo sequencial.
9. **Esquecer `shutdown()`**: o programa não termina.
10. **`run()` no lugar de `start()`**: não cria thread.
11. **Variável compartilhada acumulada por várias threads** (`soma++`): condição de corrida. Use redução com `Callable`.
12. **`get()` sem timeout** em tarefa que pode travar: o sistema fica preso para sempre. Prefira `get(tempo, unidade)`.

---

## 15. Fontes

- **Slides da aula:** Shimomura, L. C. *Computação Escalável — Aula 7 (25/09)*.
- **Código da professora:** `fontes/códigos/ExemploExecutorService/Main.java`.
- **Mattson, T.; Sanders, B.; Massingill, B.** *Patterns for Parallel Programming*. Addison-Wesley, 2004. As páginas do PDF estão entre parênteses (página impressa = PDF − 15).
  - §3.3, eficiência e distribuição *block-cyclic*, p. 36 (PDF 51)
  - §4.4, escalonamento estático × dinâmico, fila de tarefas, *work stealing*, p. 68–69 (PDF 83–84)
  - §4.6, distribuição cíclica e regra de ~10 tarefas por UE, p. 85 (PDF 100)
  - §5.5, Master/Worker; `ThreadPoolExecutor` implementa o padrão, p. 143–151 (PDF 158–166)
  - §5.10, Distributed Array: blocos 1D/2D, bloco-cíclico, p. 198–202 (PDF 213–217)
- **van Steen, M.; Tanenbaum, A. S.** *Distributed Systems*, 4ª ed. (versão 4.02).
  - §3.1, threads, custo da troca de contexto (Nota 3.1), servidor *dispatcher/worker*, p. 112–126 (PDF 128–142)
  - §3.4.4, server clusters, switch/front end, *round robin*, p. 161–164 (PDF 177–180)
- **Documentação oficial do Java (JDK 21):**
  - `Executors`: <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Executors.html>
  - `ExecutorService`: <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ExecutorService.html>
  - `Future`: <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Future.html>

---

## 16. Conclusão para revisão

### 16.1 Resumo em 10 pontos

1. **Task** é trabalho, **chunk** é porção de dados, **thread** é recurso de execução. Normalmente há **mais tasks que threads**.
2. **Granularidade** é um trade-off: tasks grandes têm pouco overhead e balanceiam mal; tasks pequenas balanceiam bem e têm muito overhead.
3. **Nº de threads:** ≈ cores para **CPU-bound**; **mais** que cores para **I/O-bound**. Deixe parametrizável e meça.
4. Threads **não são gratuitas**: criação, memória, troca de contexto (que também afeta o cache) e sincronização.
5. **Balanceamento estático** tem baixo overhead, mas a thread mais lenta dita o tempo. O **dinâmico** se adapta a custos diferentes, com custo de coordenação.
6. **Mapeamento estático:** blocos (localidade), cíclico (balanceia custo variável), aleatório (evita padrões), grafos (região + poucas arestas cortadas).
7. **Mapeamento dinâmico:** centralizado (**Master/Slave**, fácil mas com gargalo) ou distribuído (**work stealing**).
8. **Load balancer** de SD distribui conexões: **Round Robin** (estático) × **Least Connection** (dinâmico).
9. **Localidade de dados:** espacial e temporal. Leve a computação para perto dos dados.
10. Em Java: **`ExecutorService`** (pool + fila) executa **`Runnable`/`Callable`**; **`Future`** é o "ticket" do resultado; `get()` bloqueia; **`shutdown()`** é obrigatório.

### 16.2 Tabela de conceitos-chave

| Termo | Definição em uma linha | Quando usar / lembrar |
|---|---|---|
| Granularidade | Tamanho de cada unidade de trabalho | Ajuste entre overhead e balanceamento |
| CPU-bound / I/O-bound | Limitado por cálculo / por espera de E/S | Threads ≈ cores / threads > cores |
| Balanceamento estático | Divisão fixa definida antes de executar | Custos conhecidos e uniformes |
| Balanceamento dinâmico | Threads pedem trabalho durante a execução | Custos variáveis, problemas recursivos |
| Bloco | Faixas contínuas, uma por processo | Localidade; vizinhos próximos |
| Cíclico / bloco-cíclico | Elementos ou blocos distribuídos em round robin | Custo varia ao longo dos dados |
| Master/Slave | Mestre mantém a fila e distribui tarefas | Fácil; atenção ao gargalo |
| Work stealing | Cada thread tem sua fila e rouba das outras | `ForkJoinPool` |
| Round Robin / Least Connection | Ordem fixa / servidor com menos conexões | Load balancer (estático / dinâmico) |
| Localidade espacial / temporal | Vizinhos serão acessados / o mesmo dado de novo | Percorrer por linha; acumuladores |
| `ExecutorService` | Infraestrutura que gerencia pool + fila de tasks | Sempre que houver muitas tarefas |
| `Callable<V>` | Tarefa assíncrona que retorna `V` e lança exceção | Quando precisa de resultado |
| `Future<V>` | Ticket do resultado pendente | `get`, `get(timeout)`, `isDone`, `cancel` |
| `shutdown()` | Encerra o executor após as tarefas pendentes | Sempre ao final |

### 16.3 Perguntas de autoavaliação

<details><summary><b>1. Por que 4 threads (1 por partição) podem ser mais rápidas que 12 para mín/máx/média?</b></summary>

Porque as três operações são baratas e podem ser calculadas **numa única passada** pelos dados. Com 12 threads, cada partição é lida 3 vezes (mais acesso à memória e cache) e há mais overhead de threads. Se as operações fossem muito custosas, 12 poderia valer a pena. Depende da arquitetura: é preciso testar.
</details>

<details><summary><b>2. Por que, no balanceamento estático, o tempo total é o da thread mais lenta?</b></summary>

Porque o programa só termina quando **todas** as partições terminam, e no estático ninguém ajuda ninguém: as threads que terminam cedo ficam ociosas enquanto a sobrecarregada continua.
</details>

<details><summary><b>3. Qual a desvantagem do Master/Slave e como o work stealing a contorna?</b></summary>

O mestre (ou a fila central) vira **gargalo**: todos os workers disputam o mesmo ponto. No *work stealing* **cada thread tem sua própria fila** e só acessa a fila de outra quando a sua esvazia, o que distribui a coordenação.
</details>

<details><summary><b>4. Qual a diferença entre <code>submit</code> e <code>execute</code>?</b></summary>

`execute(Runnable)` apenas agenda a execução e não devolve nada. `submit(...)` aceita `Runnable` **ou** `Callable` e devolve um **`Future`**, com o qual se pode esperar o resultado, verificar se terminou ou cancelar.
</details>

<details><summary><b>5. O que <code>future.get()</code> faz se a tarefa ainda não terminou? E se a tarefa lançou exceção?</b></summary>

**Bloqueia** a thread atual até a tarefa terminar. Se a `call()` lançou uma exceção, `get()` lança **`ExecutionException`** (com a exceção original como causa).
</details>

<details><summary><b>6. Quando usar <code>newCachedThreadPool</code> em vez de <code>newFixedThreadPool</code>?</b></summary>

Para **muitas tarefas curtas e assíncronas** (ex.: I/O). O cached cria threads sob demanda e remove as ociosas após 60 s. Para trabalho **CPU-bound** prefira o fixo com ≈ nº de cores, porque o cached não tem limite e pode criar threads demais.
</details>

<details><summary><b>7. Qual distribuição estática preserva melhor a localidade e qual balanceia melhor um custo crescente?</b></summary>

**Blocos** preservam melhor a localidade espacial (faixas contínuas). **Cíclica** balanceia melhor um custo que varia ao longo do vetor. A bloco-cíclica é um meio-termo.
</details>

<details><summary><b>8. Explique a analogia da lanchonete para <code>Callable</code> e <code>Future</code>.</b></summary>

A receita é o `Callable`, entregue ao caixa (`ExecutorService`) via `submit`. O caixa devolve **na hora** uma senha, o `Future`. O lanche é preparado em segundo plano por uma thread do pool. Quando você pede o lanche (`get()`), espera até ficar pronto.
</details>

### 16.4 Checklist

**Sei explicar…**
- [ ] task × thread × chunk e por que ter mais tasks que threads
- [ ] o trade-off de granularidade e como escolher o nº de threads (CPU × I/O-bound)
- [ ] balanceamento estático × dinâmico, com o exemplo de 8 tarefas e 4 threads
- [ ] distribuição em blocos, cíclica, aleatória e de grafos
- [ ] Master/Slave (e seu gargalo) e o work stealing
- [ ] Round Robin × Least Connection
- [ ] localidade espacial × temporal

**Sei ler e corrigir código com…**
- [ ] `ExecutorService`, `newFixedThreadPool`, `submit`, `shutdown`
- [ ] `Callable`, `Future`, `get`, `get(timeout)`, `isDone`, `cancel`
- [ ] os bugs: `new Thread(callable)`, `get()` no laço de submissão, falta de `shutdown()`, `run()` × `start()`, `soma++` compartilhado

### 16.5 Ligação com a próxima aula

Na **[Aula 08](08_forkjoin_bigdata_hadoop.md)**:
- o **`ForkJoinPool`** em detalhe: `RecursiveTask`, `fork()`, `compute()`, `join()`, o limiar de granularidade e o código `BuscaMaiorValorTask`;
- e o salto de **um nó** para **um cluster**: Big Data, **HDFS** (blocos e replicação), **MapReduce** (map → shuffle → reduce) e **Spark**. Ali, o *Master/Worker*, o *particionamento* e a *localidade de dados* desta aula reaparecem em escala de datacenter.
