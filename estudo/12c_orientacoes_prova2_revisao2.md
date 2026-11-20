# P2 — O que a professora disse na 2ª revisão (aula extra)

> **Fonte:** transcrição da aula de revisão final antes da P2 ([revisao2.txt](../fontes/aulas/revisao2.txt)). `L63` = linha 63 do arquivo. A transcrição tem muitos erros de reconhecimento de fala; as correções estão no [glossário (§6)](#6-glossário-de-correções-da-transcrição).
> **O que este arquivo é:** só o registro do que ela **disse** que cai, que não cai e **como** cai. Quando preciso interpretar uma frase confusa, está marcado como **[interpretação]**. Esta aula é a **mais recente**: onde conflitar com o [12b](12b_orientacoes_prova2.md), vale este (ver [§5](#5-o-que-muda-em-relação-ao-12b)).
> **Lista:** os exercícios numerados ("Ex. 6", "Ex. 7", "Ex. 9") são os de [lista_P2.pdf](../fontes/aulas/lista_P2.pdf). **Você informou que os exercícios dessa lista que ela trabalhou em aula têm 100% de certeza de cair** (essa certeza vem de você, não de uma frase dela na transcrição). Ver [§1.1](#11-exercícios-da-lista_p2pdf).
> **Prova:** P2, 27/11.

---

## 1. Resumo rápido

| | Tópico | Como cai |
|---|---|---|
| **CAI** | Spark RDD: **DAG** de um RDD a partir de um código | Exercício: "qual o DAG do `rdd_prova`?" ([§3.1](#31-spark-rdd-dag-e-quando-executa)) |
| **CAI** | SparkSQL/DataFrame → **álgebra relacional** e **árvore de consulta** | Exercício: dado o código, escrever a consulta equivalente e/ou a árvore ([§3.2](#32-sparksql--álgebra-relacional--árvore)) |
| **CAI** | **Otimização por heurísticas** (regras) | "Quais heurísticas aplicar e por quê?" / "cite duas otimizações" ([§3.3](#33-otimização-baseada-em-regras)) |
| **CAI** | **Algoritmos de junção** (nested loops, hash, sort-merge; broadcast, shuffled hash, shuffled sort-merge) | "O algoritmo proposto é o mais adequado? Justifique; se não, sugira" ([§3.4](#34-algoritmos-de-junção)) |
| **CAI** | **Programação paralela, só conceitual:** decomposição dados × tarefas, as 4 estruturas, balanceamento estático × dinâmico | Teórica ([§3.5](#35-programação-paralela-só-conceitual)) |
| **CAI** (revisado) | ACID × BASE × CAP, e "CP/AP" | Conceitual ([§3.6](#36-acid-base-e-cap)) |
| **NÃO CAI** | **Código Java** e exercícios de **Scala puro**; "compilar no papel". **Código Spark (em sintaxe Scala) pode cair**, para ler | — ([§2](#2-o-que-ela-disse-que-não-cai)) |
| **NÃO CAI** | Exercício de **identificar decomposição/estrutura/tarefas paralelizáveis** em código | — |
| **NÃO CAI** | Renomeação (ρ) · *hash partitioning* · detalhes de **custo** · *nested loop* no Spark | — |
| **INDEFINIDO** | Pergunta "em que linha o código começa a executar de fato?" | Ela disse "vou pensar" ([§4](#4-pontos-ambíguos-ou-em-aberto)) |

### 1.1 Exercícios da lista_P2.pdf

A lista tem 9 exercícios e diz ser "**adicional** à lista passada em aula no dia 13 de novembro" (essa outra lista não está em `fontes/`). Cruzando com a transcrição:

| Ex. | Tema | Na revisão 2 | Status |
|---|---|---|---|
| 1 | **BASE**: ideia e desvantagens frente ao ACID | Revisado ([§3.6](#36-acid-base-e-cap)); não resolvido como exercício | Conteúdo coberto |
| 2 | **Grafo de dependência + estrutura de algoritmo** em pseudocódigo: (a) *merge sort* → **divisão e conquista**; (b) filtro de imagem em 4 blocos + salvar → **decomposição geométrica** | **Não foi trabalhado.** Conflita com o "não cobra" do [§2](#2-o-que-ela-disse-que-não-cai) | **Em aberto** ([§4](#4-pontos-ambíguos-ou-em-aberto), item 2) |
| 3 | **DAG** de `textFile → filter → map → reduceByKey → collect` + "para que o DAG é usado" | Mesmo tipo do exercício de DAG resolvido ([§3.1](#31-spark-rdd-dag-e-quando-executa)) | Conteúdo coberto |
| 4 | **Hadoop × Spark**: recuperação de falhas e diferença principal | Ela explicou (replicação de blocos × linhagem; [§3.1](#31-spark-rdd-dag-e-quando-executa)) | Conteúdo coberto |
| 5 | **HDFS**: 5 workers de 20 MB, bloco de 10 MB, relações de 15 e 10 linhas (1 MB cada), replicação 3 | Não mencionado | — |
| **6** | **Código Spark → álgebra relacional** (`clientes`/`pedidos`) | **Apresentado e discutido** ([§3.2](#32-sparksql--álgebra-relacional--árvore)) | **Certeza** (sua informação) |
| **7** | **Árvore inicial + 2 otimizações** (Aluno/Matrícula/Disciplina) | **Resolvido passo a passo** ([§3.3](#33-otimização-baseada-em-regras), exercício B) | **Certeza** (sua informação) |
| 8 | **Estimativa de cardinalidade errada** → execução pior | Não mencionado; ela disse que custo é "de mestrado" | — |
| **9** | **Escolha do algoritmo de junção** (Transações × Contas) | **Resolvido** ([§3.4](#34-algoritmos-de-junção), exercício 2) | **Certeza** (sua informação) |

> Os exercícios que ela resolveu e que **não** são da `lista_P2` (DAG do `rdd_prova`, pedidos × clientes, Vendas × Lojas) vêm da outra lista/slides e estão resolvidos nos [Ex. 7, 8 e 10 do arquivo 12](12_joins_spark_revisao_prova.md#exercício-7--spark-dag-e-quando-executa).

---

## 2. O que ela disse que NÃO cai

| O que | O que ela disse | Onde |
|---|---|---|
| **Código na prova** (Java, Scala, "compilar/rodar no papel") | "Pelo tamanho de coisa, **eu não vou cobrar código na prova**. Nada de compilar no papel, nem Scala, nem Java." Um aluno completou "só Spark" e ela confirmou: "**não vamos pedir código de programação paralela**… Spark também é, mas tá **só do Spark**". Reforçou no fim: "o máximo que eu vou colocar é o **Spark**" e "não vou passar código porque senão fica muito extenso a prova, já tem muito conteúdo". | L63–69, L703–711 |
| **Exercícios de Scala e de Java** (como os da aula) | "Aquela [questão] que nem o Scala que a gente viu, aquele outro exercício de [olhar o] Java… **eu não vou cobrar**. Vocês já estão fazendo isso no trabalho." | L69 |
| **Exercício de "qual a decomposição / qual a estrutura das tarefas / quais tarefas paralelizam"** (e o **grafo de dependência** feito sobre um código) | "Aquele tipo de exercício, **eu não vou cobrar**, só vou cobrar [Spark SQL e joins]." No fim: cobra "**mais a parte conceitual**, porque vocês estão fazendo o trabalho". **[interpretação]** — ver [§4](#4-pontos-ambíguos-ou-em-aberto), item 2. | L71, L667–669 |
| **Renomeação (ρ)** | Ao falar de colunas com nome repetido: "teria que usar o operador de renomeação… **mas eu não passei para vocês**, porque como vocês não tiveram em banco, ia ficar muita coisa." | L207 |
| ***Hash partitioning*** (o hash que reparte os dados no *shuffle*) | "**Não precisa lembrar dessa parte da prova**." | L285 |
| ***Nested loop* no Spark** | "Tem o *nested loops* também, mas **eu não vou falar** dele porque nunca vi acontecer." | L251 |
| **Otimização baseada em custo (profundidade)** | "A baseada em custo é muito mais complexa… estatística de banco, distribuição de dados, como calcula o tanto de tuplas… **é matéria de mestrado**." Nos exercícios ela só passou **regras**: "em regras vai ser só assim". | L603–607 |
| **Saída do Catalyst** (`explain`) **[interpretação]** | Aluno: "o Catalyst também entra, aquela saída dele?" Ela: "**não**… não vou colocar aquela saída, vou falar sobre os **algoritmos de junção**." | L73–79 |
| ***Pipeline* e *Event-Based*** | Ela listou só "**quatro** tipos de estrutura de algoritmo" (ver §3.5). Já constava como "não cai" no [12b](12b_orientacoes_prova2.md). | L697–699 |
| **Aula 5 e anteriores** | Parte de programação paralela: "é **a partir da Aula 6**"; na 5 ela só passou "alguns slides de introdução" com os capítulos dos livros. | L673–681 |

---

## 3. O que cai e como cai

### 3.1 Spark RDD: DAG e quando executa

É o **Exercício 7** do [arquivo 12](12_joins_spark_revisao_prova.md#exercício-7--spark-dag-e-quando-executa): `parallelize → filter → map → reduceByKey` (o `reduceByKey` é inferido do exercício do material; na fala está ininteligível). Duas perguntas:

1. **"Qual o DAG do `rdd_prova`?"**
   - O DAG acumula **todas as transformações** do código, **inclusive o `parallelize`**: cada linha é uma transformação aplicada à estrutura.
   - Ela **pede só as operações escritas** ("eu vou simplificar, a gente pede só as operações"). **Não precisa desenhar os dados** dentro de cada RDD.
   - **Por que o Spark mantém o DAG** (a pergunta conceitual embutida): **tolerância a falhas**. No Hadoop a tolerância vem de **replicação de blocos**; no Spark vem da **linhagem (*lineage*)**: se algo falha no `map`, ele refaz a partir do ponto anterior, porque tem a linhagem salva.
   - **Gabarito do Ex. 3 da lista ("para que o DAG é usado?"):** (1) ***lazy evaluation*** — registra a sequência de transformações antes de executar e permite otimizar o plano e encadear operações em memória quando uma **ação** é chamada, sem gravar intermediários em disco; (2) **tolerância a falhas** — mantém a linhagem e **recomputa só as partições perdidas** a partir das etapas anteriores. (O Ex. 4 da lista contrasta: **Hadoop** = disco + replicação física de blocos, recupera pelas cópias nos DataNodes; **Spark** = memória + linhagem lógica.)
   - **DAG × árvore de consulta:** a árvore de consulta só tem **operadores relacionais** (junção, seleção, projeção); o DAG do Spark tem **todos os passos do código**.
   - Se o RDD não está em cache, o Spark UI mostra toda a linhagem.
2. **"Em que linha o código começa a ser executado de fato?"** → **linha 6, o `collect`** (alunos chutaram 5 e 7). O `collect` é uma **ação** que puxa os dados de todas as partições para o **driver**; é o "**gatilho**" (*trigger*). Antes disso há só **lazy evaluation**. O `println("DAG montado com sucesso")` **aparece de qualquer jeito** (não depende do Spark).
   - **Atenção:** ela disse "**não vou cobrar o `.collect`** na prova, estou colocando só para discutir", mas em seguida alguém sugeriu que "essa é uma boa para cobrar" e ela respondeu "vou pensar". Ver [§4](#4-pontos-ambíguos-ou-em-aberto), item 1.

### 3.2 SparkSQL → álgebra relacional → árvore

**Formato:** é dado um código com DataFrames (`filter`, `join`, `select`) e pergunta-se "**qual a representação desta consulta em álgebra relacional?**" (e, no exercício seguinte, "desenhe a **árvore de consulta**"). No fim da aula ela voltou a este tipo ("**exatamente esse tipo de exercício**"): é o **Ex. 6 da lista_P2**, com um DataFrame `clientes_filtrados` e a pergunta "qual a consulta em álgebra relacional equivalente a este código?".

**Ex. 6 da lista (código e gabarito):**

```scala
val clientesFiltrados = clientes.filter($"cidade" === "Londrina").select($"id_cliente", $"nome")
val pedidosFiltrados  = pedidos.filter($"valor" > 500 && $"status" === "PAGO").select($"id_cliente", $"valor")
val resultado = clientesFiltrados.join(pedidosFiltrados, "id_cliente").select($"nome", $"valor")
```

Gabarito (os símbolos se perderam na extração do PDF; **reconstruído** a partir dos operadores que sobraram):
π<sub>nome, valor</sub>( π<sub>id_cliente, nome</sub>(σ<sub>cidade='Londrina'</sub>(clientes)) ⋈<sub>id_cliente</sub> π<sub>id_cliente, valor</sub>(σ<sub>valor>500 ∧ status='PAGO'</sub>(pedidos)) ).
Note: cada `filter` → σ (a condição `&&` vira **∧**), cada `select` → π, e o `join` por nome de coluna → ⋈ em `id_cliente`.

**Exemplo feito (pedidos × clientes):**

- `filter(valor > 100)` sobre `pedidos` → **σ** valor>100 (pedidos)
- `join(clientes, id_cliente = id)` → **⋈** com condição `id_cliente = id`
- `select(nome, valor)` → **π** nome, valor
- Álgebra: π<sub>nome, valor</sub>( σ<sub>valor>100</sub>(pedidos) ⋈<sub>id_cliente=id</sub> clientes )

**Regras que ela deu:**

- **Tradução:** `filter` = **seleção (σ)**; `select` = **projeção (π)**. (Na aula, um aluno confundiu e ela corrigiu: "seleção não é *select*, é *filter*".)
- **A álgebra se lê "de dentro para fora"; a árvore, "de baixo para cima".** Os slides dela estavam "ao contrário", por isso o enunciado pede "**uma** representação".
- **Qualquer representação equivalente vale.** Ela citou como válidas: a σ antes **ou depois** da junção ("em termos da consulta escrita, o resultado final é o mesmo"), trocar ⋈ por **× seguido de σ**, e **trocar a ordem das relações** (a junção é **comutativa**: pedidos ⋈ clientes = clientes ⋈ pedidos). "Eu só quero **qualquer** representação ali": **não** é para otimizar.
- Na projeção **não precisa** indicar de qual relação vem cada coluna (pergunta de aluno: "não tem que colocar de onde vem?" → "não"). Se houvesse **duas colunas com o mesmo nome**, aí seria preciso renomear (ρ), mas isso **não cai** (§2).
- Ela destacou que álgebra relacional é a tradução "**mais direta possível**", pois é como a consulta é representada dentro do banco; o difícil é "**virar a chavinha**" de SQL/Spark para pensar em **relações**.

### 3.3 Otimização baseada em regras

Ela passou **só** otimização por **regras/heurísticas** (§2). Dois exercícios:

**Exercício A — "Quais heurísticas aplicar e por quê?"** (consulta: π<sub>nome,valor</sub>(σ<sub>valor>100</sub>(pedidos ⋈ clientes)))

- Resposta: **selection push-down** (descer a seleção para antes da junção). **Por quê (3 razões que ela deu):**
  1. A consulta continua **equivalente** (o resultado não muda). "É toda a ideia da heurística baseada em regras."
  2. **Reduz o nº de tuplas** que entram na operação mais custosa, a **junção**.
  3. O **atributo do filtro pertence a uma só relação** (`valor` está só em `pedidos`), então dá para descer.
- **Variação que ela fez:** se houver **outra condição sobre a outra relação** (ex.: `nome = 'João'`, de `clientes`), primeiro **quebrar a seleção em duas** (cascata de seleção) e **descer cada uma** para sua relação. Se `nome` for **chave primária**, há "mais razão ainda": sobra **uma única linha**.

**Exercício B — Aluno × Matrícula × Disciplina** (**Ex. 7 da lista_P2**). Esquemas: Aluno(**ra**, nome, curso), Matricula(**ra**, **cod_disc**, ano, nota), Disciplina(**cod_disc**, nome_disc, creditos). Expressão: π<sub>nome, nome_disc, nota</sub>( σ<sub>curso='Computação' ∧ ano=2026 ∧ Aluno.ra=Matricula.ra ∧ Matricula.cod_disc=Disciplina.cod_disc</sub>( (Aluno × Matricula) × Disciplina ) ). Enunciado: (a) desenhar a **árvore de consulta inicial** e (b) "**identifique duas otimizações baseadas em regras e justifique**".

- **Gabarito da lista para (b):** (1) **selection push-down** (decompor a σ conjuntiva em cascata e descer `curso` para Aluno e `ano` para Matricula; justificativa: reduz a cardinalidade antes do produto cartesiano) e (2) **× + σ → ⋈** (o × gera |R|·|S| combinações, muitas inválidas, que a seleção descartaria logo depois). **Na revisão ela acrescentou uma 3ª opção válida** (projection push-down em Disciplina), listada abaixo; **sempre inclua a justificativa**.

- **Árvore inicial: respeite os parênteses.** A expressão tinha (Aluno × Matrícula) × Disciplina, então o 1º × é Aluno com Matrícula e o 2º é o resultado com Disciplina. "Poderia ser Matrícula × Disciplina primeiro, mas como a consulta diz que primeiro é Aluno e Matrícula, **a representação correta é essa**. Toma cuidado com os parênteses." Acima dos ×, uma **σ com todas as condições**, e no topo a **π**.
- **Otimizações válidas** (ela deu **três**; "**duas das três** seriam respostas válidas"):
  1. **Selection push-down** (depois de quebrar a σ em várias): `curso = 'Computação'` desce para **Aluno**; `ano = 2026` desce para **Matrícula** (de onde vem `ano`); as condições com atributos das **duas** tabelas (`aluno.RA = matrícula.RA`, `matrícula.cod_disc = disciplina.cod_disc`) sobem/ficam junto dos ×.
  2. **Trocar produto cartesiano seguido de seleção (com condição entre as duas tabelas) por junção (⋈).** Vale para os dois × da árvore.
  3. **Projection push-down** na **Disciplina**: ver quais atributos de cada relação serão usados depois. Aluno (curso, RA, nome) e Matrícula (nota, ano, RA, cod_disc) usam **tudo**, então nada a projetar. Em Disciplina só se usam `cod_disc` e `nome`; **descarta `créditos`** antes da junção.
- **O que NÃO conta como otimização:** a **cascata de seleção** sozinha. "É uma **equivalência** da álgebra relacional; quando executa, executa exatamente igual. O tipo de otimização de fato é o *select push-down*."
- **Trocar a ordem dos produtos/junções** (ex.: Matrícula com Disciplina primeiro): é **equivalente**, e seria otimização **só se justificar com o nº de tuplas** das relações intermediárias. Como o enunciado só dava as chaves primárias, "não consigo afirmar de fato". Ou seja, **sem tamanhos, não arrisque como resposta**.
- **Por que só regras:** heurísticas "geralmente funcionam" (o *push-down* "funciona muito"); o banco real usa **heurísticas + custo**, e o custo estima quantas tuplas sobram (isso é o que ela **não** aprofunda).

### 3.4 Algoritmos de junção

**Conceitos que ela revisou** (e o que "tem que lembrar"):

| Algoritmo | Ideia | O que lembrar |
|---|---|---|
| **Nested loops** | um `for` dentro de outro | o mais básico possível |
| **Hash join** | monta uma **tabela hash** de uma relação (em geral a **menor**) sobre a **chave de junção**; aplica a **mesma função** à outra relação e casa | **precisa de espaço em memória** para a tabela hash |
| **Sort-merge join** | **ordena** as duas pela chave de junção e anda com ponteiros comparando | **custo adicional de ordenação** |

**No Spark** (os que ela vai considerar): **Broadcast (Hash) Join**, **Shuffled Hash Join** e **Shuffled Sort-Merge Join** (*nested loops* não, ver §2).

| | Quando | Vantagem / custo |
|---|---|---|
| **Broadcast (hash) join** | **uma tabela é pequena** o bastante (padrão do Spark: cerca de **10 MB**). Envia-se uma **cópia inteira** dela a todos os nós/partições; em cada partição faz-se a junção **local** (tabela hash local) e depois **concatena-se** o resultado | **Sem *shuffle*** (a parte custosa); só há a movimentação da cópia, bem mais barata. **Desvantagem:** uma das tabelas **tem de ser pequena**, o que limita o uso em larga escala |
| **Shuffled hash join** | **as duas são grandes** (não dá para *broadcast*) | há *shuffle*; cada partição **monta uma tabela hash**, então **todas precisam ter memória** suficiente (custo a mais) |
| **Shuffled sort-merge join** | **as duas são grandes** | há *shuffle*; **não monta tabela hash**, mas **ordena as chaves** dentro de cada partição (custo a mais). É escolhido também por **economia de memória** ("memória é uma coisa preciosa") e quando os dados **já estão agrupados/ordenados** (ex.: depois de um `groupBy`) |

**Sobre o *shuffle* (explicação dela):** reparticiona os dados para que as **mesmas chaves das duas tabelas caiam na mesma partição**; em cada partição roda-se o algoritmo. **O objetivo não é terminar com uma partição só:** o resultado fica espalhado pelas partições (no *broadcast*, nas da tabela maior).

**Formato do exercício:** é dada a descrição das relações (nº de tuplas, tamanho, como estão particionadas/ordenadas, memória disponível) e o **plano físico proposto pelo otimizador**. Pergunta: "**o algoritmo proposto é o mais adequado? Justifique; se não, sugira o melhor**."

**Exercício 1 — Vendas × Lojas** (é o [Exercício 10 do arquivo 12](12_joins_spark_revisao_prova.md#exercício-10--escolha-do-algoritmo-de-join)). Vendas: 500 mi de tuplas, ~40 GB, aleatoriamente em 320 partições. Lojas: 50 tuplas, ~2 KB. Plano proposto: **shuffled sort-merge** (redistribui os 40 GB pela rede).

> **Resposta-modelo dela:** "O algoritmo proposto **não é o mais adequado**, pois como existe uma tabela pequena de aproximadamente 2 KB, ela pode ser enviada através de **broadcast** e, por consequência, não realizar o processo de ***shuffle***, que costuma ser **mais custoso que o broadcast**, principalmente para dados pequenos. O adequado seria **Broadcast Join** (ou Broadcast Hash Join); **pode colocar só "broadcast join"**."
>
> Esclarecimentos dados: manda-se a tabela **inteira** a todos (não só uma parte); a tabela hash é construída **dentro de cada partição**, sobre as chaves de junção das 50 tuplas.

**Exercício 2 — Transações × Contas** (**Ex. 9 da lista_P2**; resolvido no [arquivo 12, Exercício 11](12_joins_spark_revisao_prova.md#exercício-11--transações--contas-lista-p2-ex-9--certeza)). Transações: 400 mi de tuplas, 45 GB; Contas: 60 mi de tuplas, **8 GB**; **as duas particionadas e pré-ordenadas pelo atributo `id_conta`**. **Memória por executor para a tabela hash limitada a 512 MB.** Plano proposto: **shuffled hash join**.

> **Resposta-modelo dela:** **Sort-merge join** é o mais adequado, "pois lida de forma escalável com **duas relações grandes**, sem exigir que uma delas caiba integralmente na memória para construir uma tabela hash. Além disso, como as tabelas **já estão pré-ordenadas** pelo atributo de junção, **o custo de ordenação é eliminado**, permitindo realizar a junção por uma simples **varredura sequencial** (a fase *merge* do algoritmo)."
>
> - **"O pulo do gato" = a pré-ordenação.** Pensando em custo: não precisa montar a tabela hash **nem** ordenar; só "casar" as duas.
> - Mesmo **se não estivessem ordenadas**, ela acha que o sort-merge ainda seria o mais adequado (a tabela hash de 45 GB não cabe em 512 MB); a ordenação custaria algo como n log n, mas o Spark tem boa implementação de ordenação.
> - O enunciado trazia uma informação "só para assustar" que **não é usada** (L611; ver [§4](#4-pontos-ambíguos-ou-em-aberto)).

### 3.5 Programação paralela: só conceitual

Ela disse que cobra "**mais a parte conceitual**", porque a prática está no trabalho e porque "aplicado mesmo não tem como, a não ser que eu passe código" (e não vai passar). O que nomeou explicitamente:

- **Decomposição:** só **dados** e **tarefas** ("só dados e tarefas").
- **Estruturas de algoritmo: 4 tipos** — **divisão e conquista**, **paralelismo de tarefas**, **geométrico** e **recursivo (dados recursivos)**.
- **Balanceamento de carga: estático × dinâmico**, com a pergunta típica "qual a **vantagem** de um, qual a **desvantagem** do outro".
- **Granularidade e mapeamento** foram citados por um aluno ("é granularidade, balanceamento de carga, mapeamento…"), e ela respondeu de forma geral ("o que eu posso cobrar é a parte conceitual"). Estão no escopo pela lista de tópicos da [1ª revisão](12b_orientacoes_prova2.md), mas **ela não os repetiu** nesta aula.
- **Grafo de dependência — definição:** a aresta vai da **tarefa precedente para a tarefa que consome a sua saída**. "Se T3 consome a saída de T1, a aresta é **T1 → T3**." Ela confirmou que a resolução da aula passada "tá certa" (isto **resolve a dúvida da direção da seta** que ficou aberta no [12b §2.3](12b_orientacoes_prova2.md#23-exercício-grafo-de-dependência-processavendas)).

### 3.6 ACID, BASE e CAP

Ela **revisou** o tema (porque estava "faltando" da aula anterior) e fechou com "**lembra aí das nomenclaturas, por favor**". Não disse "isto cai" de forma literal, mas o [12b §4](12b_orientacoes_prova2.md#4-nosql-acid--base-e-cap) já registra que cai. O que ela reforçou:

- **ACID × BASE — "a diferença principal que a gente precisa lembrar":**
  - **ACID** (transação **commitada**: ou atualiza todo mundo ou ninguém; ela citou o *two-phase commit* da prova anterior): garante **consistência**.
  - **BASE**: garante **disponibilidade** — "estou sempre disponível para responder; mas, quando você faz uma transação, **pode ser que eu não atualize na hora**: respondo e continuo **atualizando em background**". **Problema:** uma consulta feita **antes do fim da atualização** pode ver **dados inconsistentes momentaneamente**.
- **CAP:** em sistemas **distribuídos** é impossível garantir **simultaneamente** os 3:
  - **C**onsistência: todos os nós retornam **a mesma informação atualizada, no mesmo instante**;
  - **A**vailability (disponibilidade): **toda requisição recebe resposta**, mesmo que **não seja a versão mais recente**;
  - **P**artition tolerance (tolerância a particionamento): o sistema **continua funcionando** mesmo com falhas de comunicação entre nós.
  - **Falhas de rede são inevitáveis ⇒ o P é obrigatório ⇒ só se escolhe entre C e A** (sistema **CP** ou **AP**).
- **Exemplo do banco** (transferência de A para B com um nó falhando no meio): **CP** → a transação **não é commitada** e o cliente **fica sem resposta** sobre o saldo; **AP** → o cliente **recebe resposta, mas com saldo incorreto** (o *background* não terminou).
- **Nomenclatura:** **BASE ≈ sistemas AP** (disponíveis, "eventualmente" consistentes). **ACID ≈ sistema CP** (consistente).
- **CAP × BASE:** "bastante parecidos"; **CAP** é falado no contexto de **sistemas distribuídos em geral**, e **BASE** vem como o teorema/modelo do lado de **bancos de dados**, em contraponto ao ACID.

---

## 4. Pontos ambíguos ou em aberto

1. **A pergunta "em que linha executa de fato" vai cair?** Ela disse que não vai cobrar o `.collect`, mas alguém falou "pode cobrar esse / é uma boa para cobrar" e ela: "vou pensar" (L127–147). **Indefinido.** Na dúvida, saiba que **só uma ação dispara a execução** (`collect`, `count`, `show`, escrita…) e que **transformações são *lazy***.
2. **Grafo de dependência e "qual decomposição?" sobre código.** **Atenção:** o **Ex. 2 da lista_P2** pede exatamente isso (grafo + estrutura do algoritmo), só que em **pseudocódigo** (merge sort → divisão e conquista; filtro de imagem em blocos → decomposição geométrica), e ela **não o trabalhou** na revisão. O "não cobra" dela fala de **código Java/Scala**; o pseudocódigo pode não estar incluído. **Na dúvida, estude o Ex. 2 da lista** (a diferença-chave que o gabarito destaca: D&C tem **recursão + etapa de combinação**; geométrica = blocos independentes, **sem recursão e sem combinar**). Em L71 ela diz que **não** cobra "o tipo de exercício de ver qual a decomposição, qual a estrutura das tarefas, qual tarefa pode paralelizar". Em L669 (quase no fim) ela cita "faça o grafo de dependência, quais tarefas podem ser executadas em paralelo, estou usando decomposição de dados ou de tarefas, explique o porquê" ao dizer que **cobra a parte conceitual** e que o resto vocês "já fazem no trabalho". **[interpretação]:** a frase descreve o **tipo de exercício que não será pedido** (é o que os alunos já praticam no trabalho). A fala é confusa; **não aposte tudo nisso**: saiba pelo menos **definir** o grafo (§3.5) e a diferença entre as decomposições.
3. **"Catalyst também entra?"** (L73–79): resposta truncada; interpretei como "não vou pedir aquela saída (`explain`), vou pedir algoritmos de junção". Ainda assim, ela explicou que o **DataFrame passa pelo Catalyst (otimizador)** e o **RDD não** (ver abaixo, §5 item 5).
4. **"Três pontinhos" na árvore** (L419–425): enquanto copiava as condições ela usou "...", um aluno perguntou se podia na prova e alguém respondeu "**não, não pode**". Não dá para saber quem falou; **[interpretação]:** na prova, escreva as **condições completas**.
5. **Trocar a ordem dos produtos/junções como "otimização"** (L505–537): só vale justificada por **contagem de tuplas**; sem tamanhos no enunciado, ela não considerou conclusivo.
6. **Dado "só para assustar"** (L611): no enunciado do exercício das Transações ela disse "esse aqui no final **a gente não vai usar**, só para assustar". Não dá para saber qual item é; lembre que **o enunciado pode trazer informação irrelevante**.
7. **Dificuldade da lista × prova** (L617–625): trecho **ininteligível** na transcrição; nada a concluir.
8. **"Não cai uma questão dessa, né?"** (L345): um aluno perguntou sobre a questão de árvore/otimização que ela ia resolver; **a resposta não aparece** na transcrição.
9. **Custo e cardinalidade:** a lista de tópicos da 1ª revisão cita "ideia geral da otimização baseada em custo e estimativa de cardinalidade". Nesta aula ela diz que **só ensinou regras** e que custo é "de mestrado". Espere, no máximo, **a ideia geral** (o banco usa heurísticas **e** custo; o custo **estima tuplas**).
10. **Citado só de passagem (sem dizer se cai):** **cálculo relacional de tuplas** (L731–733: "linguagem ainda mais alta nível, você só descreve o que quer"; ela só mencionou). Não é conteúdo trabalhado.

---

## 5. O que muda em relação ao 12b

| Tema | 12b (1ª revisão) | 12c (esta aula) — **prevalece** |
|---|---|---|
| Spark, SparkSQL, álgebra/otimização, joins | **Pendente** (aula extra) | **Aconteceu**; ver §3.1–3.4 |
| Código na prova | "Código aparece para **ler e interpretar**" (Java com `ExecutorService`) | **Sem código**, exceto **Spark** |
| Ler o `RastreioPool` (nº de tarefas/threads/decomposição) | Prioridade **Alta** | Exercício de "qual decomposição/qual estrutura" sobre código: **ela disse que não cobra** (§2; ver ressalva §4 item 2) |
| Grafo de dependência (ProcessaVendas) | Prioridade **Alta**, como exercício | Como exercício sobre código: **não cobra** (**[interpretação]**, §4 item 2). **Definição** e direção da seta: **T1 → T3** (precedente → consumidor), **confirmada** |
| Direção da seta | "Ela ficou em dúvida" | **Resolvida** (§3.5) |
| Estruturas de algoritmo | Tarefas e dados; "flow of data" não cai | Mesmo: **4 tipos** (D&C, task parallelism, geométrico, recursivo) |
| HDFS, MapReduce, 5 Vs, NameNode/DataNode, NoSQL | Cai | **Não foram mencionados** nesta aula (nada muda) |

---

## 6. Glossário de correções da transcrição

| Na transcrição | Quer dizer |
|---|---|
| ácid, ácido, acide, ACID | **ACID** |
| base, bas, BS, basic, "teorema BASE" | **BASE** (é um modelo/filosofia; o teorema é o CAP) |
| queda, CP, "teorema CP", "teorema Cap" | **teorema CAP** |
| "consistência, disponibilidade e **persistência**" (fala de aluno, L3) | o P é **tolerância a particionamento** (*partition tolerance*), **não** persistência |
| "sistemas de circuito" | sistemas **distribuídos** |
| "no ciclo", "ciclo" | **NoSQL** |
| tio Face comite | ***two-phase commit*** (2PC), visto na P1 |
| Spongebre / Spongebob | **Stonebraker** (crítica ao MapReduce; só anedota) |
| Spark Sec, Sparkle, SP, Spark Stickle | **Spark SQL** |
| catal, catalist | **Catalyst** |
| Dag, DAG do RTD, RDD prova | **DAG**, **RDD**, `rdd_prova` |
| colete, collet, "ponto colete" | **`.collect()`** |
| las evaluation | ***lazy evaluation*** |
| "rador", Radup | **Hadoop** |
| ponto underline, "ponto underline 1/2" | `._1`, `._2` (campos da tupla/par chave-valor) |
| prelacional, "para relacionar", "plano de relacionar" | **álgebra relacional** |
| plano cartesiano | **produto cartesiano** (×) |
| pushd, postal, custal, "select custal", "selectinal", cogital | ***(selection) push-down*** |
| "cascada/cascata de seleção" | **cascata de seleção** (equivalência) |
| J, joia, joint, Joy | **join** |
| hash Joy, hash join, Hooyin, Hin | **hash join** |
| short merge, sortm Joy, sort join, "short merch" | **sort-merge join** |
| "nesses loops", "loops alinhados", Ness/N Loops | ***nested loops*** (laços aninhados) |
| shampoo, shavô, chavô, "shuffle" | ***shuffle*** |
| brc Join, "broadcast hashoo" | ***broadcast (hash) join*** |
| tabela R, RH, "tabela R alimente" | **tabela hash** |
| tupas, túas, turmas, duplas, fontos | **tuplas** |
| "2 para B", KB, "10 m", 512 | **2 KB**, **10 MB**, **512 MB** |
| R/RA, ra, "aluno ponto ra" | **`aluno.RA`** (registro acadêmico) |
| codisk, cot disc, cod disc | **`cod_disc`** |
| "ano 2020 seis", "ano 26" | **ano = 2026** |
| curso "comunicação"/"computação" | **curso = 'Computação'** |
| "grato", "grafo de dependência de dados" | **grafo** de dependência de **tarefas** |
| "workers do club" | *workers* do **cluster** |
