# Trabalho Final — Computação Escalável (2COP022)

> **Fontes:**
> - `Aulas/Trabalho_Final-Instruções.pdf` (7 slides, lidos inteiros; o p7 também como imagem).
> - `Aulas/CE-Trabalho_Final.txt` (transcript) e `Aulas/CE-Trabalho_Final.md` (sumário).
>
> **Para que serve este arquivo:** é o ponto de partida de uma sessão à parte, em que o grupo vai
> trabalhar com o Claude para executar o trabalho. Reúne o enunciado, as regras, as decisões em
> aberto, a metodologia do teste de escalabilidade e um checklist. **Leia a seção 3 (uso de IA)
> antes de começar**: ela define o que o Claude pode e o que não pode fazer nesse trabalho.
>
> Gerado em **2026-10-10**.

---

## 1. O trabalho em uma frase

**Implementar dois algoritmos de mineração ou processamento de dados em ambiente paralelo ou
distribuído e analisar a escalabilidade das soluções conforme o volume de dados aumenta.**

A professora deixou claro que o foco **não** é a dificuldade do algoritmo: *"eu não quero um
algoritmo super, hiper, mega complicado. Esse não é o foco do trabalho."* O objetivo é que o
grupo *"vivencie o processo de implementação, teste e análise de escalabilidade"*, ou seja, passar
da ideia de *"como esse algoritmo seria implementado"* para *"vamos ver como implementá-lo agora"*.

---

## 2. Cronograma ⚠️

| Data | Entrega | Faltam (a partir de 10/10) |
|---|---|---|
| **até 25/10** | **Formulário inicial** (Google Forms no Classroom): integrantes, tarefa, algoritmos, divisão das duplas e **link do repositório GitHub** (já criado, pode estar vazio) | **15 dias** |
| **até 12/11** | **Relatório final** (template no Classroom). Na fala: "11 ou 12/11" | 33 dias |
| **13/11** (sexta) | **Apresentação**, ~20 min por grupo (~5 min por pessoa no grupo de 4) | 34 dias |
| 27/11 | **Prova 2** (não é do trabalho, mas disputa o mesmo tempo) | 48 dias |

- O dia 20/11 é feriado (Consciência Negra). Por isso a apresentação foi antecipada para o dia 13.
- **A escolha de algoritmos é por ordem de registro** (§4). Quanto antes o formulário, mais chance
  de ficar com a combinação desejada.
- O repositório será **acompanhado durante o desenvolvimento**, não só no fim (§3 e §9).

---

## 3. Uso de IA: o que é permitido ⚠️⚠️

Esta é a regra mais importante para a sessão conjunta com o Claude. Slide p6 e fala:

| | Permitido? |
|---|---|
| Implementar o **algoritmo distribuído ou paralelo** ("a peça principal do trabalho") | **NÃO** |
| Funções de **leitura inicial do arquivo** (e, pela fala, leitura e escrita do resultado final) | Sim |
| **Scripts para automatizar os testes** de escalabilidade | Sim |
| Scripts para **gerar os gráficos** dos resultados finais | Sim |

- *"Eu não vou permitir a implementação do algoritmo de distribuição paralela […] Afinal, essa é a
  peça principal do trabalho."* O slide diz que a leitura do arquivo é permitida **"e apenas
  isso"** (além dos scripts de teste e de gráfico).
- **A professora vai verificar o repositório.** Ela quer ver um **histórico de commits
  progressivo**, e não *"um commit perfeito"* uma hora antes do relatório. *"Já reprovei gente que
  usou GPT."*
- **É obrigatório declarar**, no **README** e no **relatório**, se a IA foi usada, **para quê** e
  **em quais arquivos**.

### 3.1 Como isso se traduz na sessão com o Claude

| O Claude **pode** | O Claude **não deve** |
|---|---|
| Escrever a **leitura do dataset** (CSV → estrutura em memória) e a **escrita da saída** | Escrever, completar ou "corrigir" o código **paralelo/distribuído** dos algoritmos (divisão do trabalho, threads, tarefas Fork/Join, operações Spark do algoritmo) |
| Escrever os **scripts de teste** (rodar cada tamanho, repetir, salvar CSV de tempos) | Gerar a versão sequencial do algoritmo para vocês paralelizarem (o algoritmo em si também é núcleo do trabalho) |
| Escrever os **scripts de gráfico** (tempo × tamanho, 2 linhas por dataset) | Transformar a descrição de vocês em código do algoritmo |
| Ajudar a montar o **ambiente** (JDK, Spark local, Docker), e a **estrutura do repositório** e do README | Escrever as seções do relatório que explicam as decisões de paralelismo como se fossem do grupo |
| **Explicar a teoria da disciplina** (Tópicos 25–35) e revisar se o raciocínio do grupo é coerente com ela | |
| Ajudar a **interpretar** gráficos e resultados, e a revisar texto **escrito pelo grupo** | |

Regras práticas para a sessão:
1. **Cada arquivo gerado pelo Claude vai num commit próprio**, com a mensagem dizendo isso (ex.:
   `scripts de teste (gerado com IA)`), e entra na lista da seção "Uso de IA" do README.
2. **O código do algoritmo é escrito pelas duplas**, em commits pequenos e frequentes, ao longo
   das semanas.
3. Explicações de conceito ou de erros de compilação genéricos são ajuda de estudo. Na dúvida
   se algo cruza a linha, **perguntar à professora** antes e registrar no README.
4. Este material não traz código dos algoritmos. As seções 5 e 10 só apontam **onde está o
   paralelismo** e **qual teoria justifica cada decisão**: a decisão e o código são do grupo.

> Os pontos cinzentos (ex.: o Claude explicar por que uma implementação do grupo ficou lenta) não
> estão no enunciado. A interpretação conservadora é a desta seção. Se a professora permitir mais,
> registrar a permissão no README.

---

## 4. Requisitos (checklist do enunciado)

### 4.1 Grupo e escolha

- [ ] **Grupos:** 5 grupos de 4 e 1 grupo de 3 (23 alunos).
- [ ] **Tarefa:** 1 tipo de tarefa de Ciência de Dados (clustering, classificação, regressão,
      associação etc.).
- [ ] **Grupo de 4:** **2 algoritmos diferentes** para a mesma tarefa; **cada dupla implementa
      1**.
- [ ] **Grupo de 3:** **1 algoritmo com 2 decomposições diferentes** (de dados ou de tarefas, ex.:
      dois métodos distintos de particionamento). *"Não é justo a mesma pessoa implementar dois
      algoritmos."*
- [ ] **Exclusividade:** dois grupos **não** podem escolher a **mesma combinação de algoritmos**
      (a mesma **tarefa** pode). Vale a ordem de registro, ou um acordo em sala.

### 4.2 Implementação

- [ ] **Linguagem:** **Java** (Threads, ExecutorService e ForkJoin, como em aula) **ou Scala
      sobre o Apache Spark**. **Os dois algoritmos do grupo usam obrigatoriamente a mesma
      linguagem e o mesmo framework.**
  - Na fala, outro framework (ex.: Flink) é aceito **se todo o grupo usar o mesmo**: *"tem que
    haver um consenso dentro do próprio grupo"*.
- [ ] **Mesma interface de execução** e **mesmo formato de saída** para os dois algoritmos (ou
      as duas decomposições), para uma comparação justa (§7).

### 4.3 Dados e testes

- [ ] **2 datasets distintos**, reais ou sintéticos.
  - Sintéticos: **distribuições diferentes** (a fala cita também características e número de
    features diferentes).
  - Reais: conferir se **servem para a tarefa** escolhida.
- [ ] **Tamanho:** de **10.000 a 150.000 pontos** (pelo menos na casa dos 100 mil).
- [ ] **Teste de escalabilidade** em cada dataset, aumentando o tamanho aos poucos (§8).
- [ ] **2 gráficos**: um por dataset, com **2 linhas** (uma por algoritmo ou decomposição).
- [ ] **Todos os testes na mesma máquina**: *"vocês vão ter que se coordenar para realizar esse
      teste"*.

### 4.4 Entregáveis

- [ ] **Repositório GitHub** com: os códigos-fonte dos dois algoritmos; os scripts de execução
      dos testes; um **README.md completo** (como compilar, como rodar os algoritmos e os
      testes); a declaração de uso de IA.
- [ ] **Relatório final** (template no Classroom; §11).
- [ ] **Apresentação** de ~20 min (§11).

---

## 5. Escolha da tarefa e dos algoritmos

### 5.1 Sugestões do enunciado (slide p4)

| Tarefa | Algoritmos | Observação da professora |
|---|---|---|
| Clustering | **k-means** e **DBSCAN** | — |
| Clustering 2 | **Hierárquico** e **k-Medoids** | No k-Medoids dá para usar um particionamento *"um pouco diferente do que fariam no K-means"* |
| Regras de associação | **Apriori** e **FP-Growth** | Serão vistos com o professor Anderson em ~2 semanas (a partir da aula do trabalho) |
| Busca k-NN | **LSH** e **k-NN força bruta** | No força bruta, **exigido particionamento eficiente/inteligente**, *"não aquele aleatório"* |
| Mineração de texto | **TF-IDF** e **CountVectorizer** | — |
| Supervisionado | **Random Forest** | Sugerido para o **grupo de 3** (duas decomposições); *"um pouco mais complicado"* |

Na fala, apareceu também a **PCA** (*"trabalhando com inversão de matriz"*). Outros algoritmos são
aceitos, desde que a combinação não repita a de outro grupo.

### 5.2 Como comparar as opções (critérios para o grupo decidir)

Os critérios abaixo vêm da teoria da Prova 2. A avaliação de cada opção cabe ao grupo.

| Critério | Pergunta | Teoria |
|---|---|---|
| Onde está o paralelismo | O trabalho se divide pelos **dados** (pontos, transações, documentos) ou por **tarefas** (árvores, candidatos, consultas)? | Tópico [25](25_decomposicao_tarefas_dados.md) |
| Dependências | As partes são independentes ou precisam trocar resultados a cada passo (iterações, vizinhanças)? | Tópico [26](26_analise_dependencias.md) |
| Padrão | Paralelismo de tarefas, dividir para conquistar, decomposição geométrica, dados recursivos? | Tópico [27](27_padroes_estrutura_algoritmo.md) |
| Balanceamento | As partes têm custo parecido? Se não, mapeamento estático ou dinâmico? | Tópicos [29](29_balanceamento_carga_particionamento.md) e [30](30_mapeamento_dinamico_localidade_dados.md) |
| Agregação | O resultado final é uma **combinação por chave** (estilo map/reduce)? | Tópico [35](35_mapreduce.md) |
| Custo de crescer | Como o custo cresce com n: linear, n log n, n²? Isso aparece direto no gráfico | §8.4 |
| Contraste entre os dois | As duas implementações vão escalar de forma **diferente o bastante** para gerar discussão? | §8.4 |

> **Dica da própria professora (no exemplo do slide p7):** o interessante é quando as curvas
> formam grupos com **crescimentos diferentes** (ex.: métodos exatos × aproximados). Escolher dois
> algoritmos com custos assintóticos diferentes rende uma discussão melhor no relatório.

### 5.3 Pontos de atenção por família (sem soluções)

- **Algoritmos iterativos** (k-means, k-Medoids): cada iteração depende da anterior. Há uma
  **barreira** por iteração (Tópico 26). O número de iterações precisa ser **controlado** para a
  comparação ser justa entre tamanhos (§8.2).
- **Algoritmos baseados em vizinhança** (DBSCAN, k-NN): o custo ingênuo é **O(n²)**. O
  particionamento (espacial, por exemplo) é o ponto central, e o enunciado do k-NN **exige** um
  particionamento inteligente.
- **Hierárquico:** custo e memória crescem rápido com n (matriz de distâncias). Verificar se
  150 mil pontos são viáveis na máquina do grupo **antes** de escolher.
- **Associação** (Apriori, FP-Growth): "pontos" = **transações**. Os dois algoritmos têm
  estruturas bem diferentes (geração de candidatos × árvore), o que gera contraste.
- **Texto** (TF-IDF, CountVectorizer): "pontos" = **documentos**. É o mais próximo do word count
  do Tópico 35.
- **Random Forest:** as árvores são independentes entre si, mas cada uma é cara; as duas
  decomposições do grupo de 3 podem ser por tarefas (árvores) ou por dados.

---

## 6. Linguagem e ambiente

| | **Java** (Threads, ExecutorService, Fork/Join) | **Scala + Apache Spark** |
|---|---|---|
| Visto em aula | **Sim** (Tópicos [31](31_executorservice_callable_future.md) e [32](32_fork_join_work_stealing.md), com código rodado) | **Ainda não**: virá nas próximas aulas (Spark Core e Spark SQL) |
| Tipo de paralelismo | Memória compartilhada, uma máquina | Distribuído (modo local numa máquina, ou mini-cluster em Docker) |
| Medição de tempo | `System.nanoTime()`; cuidado com o aquecimento da JVM (§8.3) | Excluir a inicialização do Spark (`SparkSession`) e a leitura (§8.3) |
| Risco de cronograma | Menor: a ferramenta já é conhecida | Maior: aprender Spark e Scala até o fim de outubro |
| O que conta a favor | Controle fino de threads, granularidade e balanceamento | Mais próximo de "Big Data" real; shuffle e particionamento visíveis |

- **Uma máquina só é aceito.** *"O Spark dá para rodar na mesma máquina, mas às vezes não é tão
  eficiente. Dá para montar com Docker um mini cluster"* (mestre e escravos em contêineres). A
  professora lamentou não ter um cluster para a turma.
- **Decisão do grupo, registrada no formulário.** A escolha vale para os dois algoritmos.
- Ambiente já disponível nesta máquina: **JDK 24** (usado em todos os códigos de `codigo/`).

---

## 7. Interface comum e formato de saída

Exigência: *"definam: esta é a forma como vou chamar meu algoritmo, esta é a forma como ele será
executado"*. Pode ser *"um shell script"* ou *"um script como se fosse uma biblioteca que chama
apenas uma funçãozinha"*. O objetivo é que não haja *"gargalo de linguagem, forma de chamada
diferente ou algo assim"*.

O grupo precisa combinar, **antes** de as duplas começarem:

| Item | O que definir |
|---|---|
| **Chamada** | Os mesmos parâmetros para os dois algoritmos. Ex.: algoritmo, arquivo do dataset, número de pontos n, número de threads (ou de partições), parâmetros do algoritmo, semente aleatória |
| **Entrada** | O mesmo formato de arquivo para os dois (ex.: CSV com cabeçalho), lido pela mesma função (pode ser feita com IA) |
| **Saída do algoritmo** | O mesmo formato (ex.: CSV com o rótulo ou resultado de cada ponto), para conferir que os dois produzem resultados válidos |
| **Saída de medição** | Uma linha por execução, igual para os dois: dataset, algoritmo, n, threads, repetição, tempo em ms |
| **Tamanhos de n** | A mesma lista para os dois (ex.: 10k, 20k, …, 150k) |
| **Máquina e configuração** | A mesma máquina, o mesmo número de threads, sem outros programas pesados rodando |

> Este "contrato" é infraestrutura. A **interface** (assinatura, parâmetros, formato) pode ser
> combinada com ajuda; a **implementação** de cada algoritmo atrás dela é das duplas (§3).

---

## 8. Teste de escalabilidade (slide p7) ⭐

### 8.1 O procedimento

1. Dado um dataset, **aumentar o tamanho aos poucos** (ex.: de 10 mil em 10 mil) até o total.
   *"Pega os primeiros 5 mil, pega os próximos 10 mil"*: são **prefixos do mesmo dataset**.
2. Em cada ponto, **rodar o programa e registrar o tempo de execução**.
3. Gerar **um gráfico por dataset**: eixo X = tamanho do dataset, eixo Y = tempo de execução,
   **2 linhas** (uma por algoritmo ou decomposição).
4. Repetir para o segundo dataset. No total: **2 gráficos**.

### 8.2 Cuidados para uma comparação justa

| Cuidado | Por quê |
|---|---|
| **Prefixos aleatorizados uma vez** | Se o arquivo estiver ordenado (por classe, por região), os primeiros 10 mil não representam o todo. Embaralhar o arquivo **uma vez**, com semente fixa, e então tirar prefixos |
| **Mesma máquina, mesma carga** | Exigência da professora; notebook na tomada, sem outros programas pesados |
| **Mesmo número de threads/partições** em todos os pontos | O teste é de **tamanho**, não de número de threads |
| **Parâmetros fixos** (k, eps, suporte mínimo, nº de árvores) | Mudar um parâmetro no meio muda o custo |
| **Algoritmos iterativos: mesmo critério de parada** | Fixar o número de iterações (ou registrar quantas ocorreram), senão um ponto pode convergir em 3 iterações e outro em 30 |
| **Repetições** | Rodar cada ponto **várias vezes** (ex.: 5) e usar a **mediana**; mostrar a variação se possível |
| **Semente aleatória fixa** | Resultados e tempos reprodutíveis |
| **Conferir a saída** | Um algoritmo rápido e errado não vale; validar contra um caso pequeno conhecido |

### 8.3 O que entra no tempo (e o que não entra) ⚠️

*"Marcar realmente apenas o tempo de execução do seu programa."*

| **Não** entra | **Entra** |
|---|---|
| Leitura inicial do arquivo para a memória | O algoritmo paralelo/distribuído, do início ao resultado |
| Inicialização do framework (`SparkSession`, driver) | |
| Escrita do resultado em arquivo (por coerência com a leitura) | |

- **Java:** usar `System.nanoTime()` imediatamente antes e depois do algoritmo, *"aquele que serve
  só para marcar o tempo de execução, que a gente estava usando na sala"*. **Complemento:** a JVM
  compila o código "quente" durante a execução (JIT). As primeiras execuções são mais lentas.
  Fazer **1–2 execuções de aquecimento descartadas** antes de medir.
- **Spark:** as operações são **preguiçosas** (*lazy*), só executam quando uma ação pede o
  resultado. Medir em volta da **ação** que dispara o trabalho, e garantir que a leitura já foi
  materializada antes do cronômetro (complemento: será visto nas aulas de Spark).

### 8.4 Lendo o gráfico (o que discutir no relatório)

- **Forma da curva:** cresce linearmente? Mais rápido que linear (n log n, n²)? Muda de inclinação
  em algum ponto? A professora quer exatamente esse tipo de frase: *"Neste ponto, quando o dataset
  cresceu, a eficiência caiu."*
- **Curvas separadas em grupos:** no exemplo do slide (gráfico de um artigo dela), os métodos
  **aproximados** ficam bem abaixo dos **exatos** e crescem de outro jeito. *"Era esperado que
  vocês notassem isso."*
- **Escala logarítmica no eixo Y:** o exemplo usa log quando os tempos diferem em ordens de
  grandeza. Se as duas linhas forem muito diferentes, considerar log em Y e **dizer isso na
  legenda**.
- **Explicar com a teoria:** custo de comunicação e de sincronização, granularidade,
  desbalanceamento, memória ou cache, shuffle (no Spark). Ver §10.
- **Opcional (vai além do pedido):** um gráfico de tempo × número de threads para um n fixo, com
  o **speedup** (Tópico 02), se sobrar tempo. Não substitui os 2 gráficos exigidos.

---

## 9. Repositório GitHub

**Criar já** (o link vai no formulário de 25/10, mesmo vazio). Estrutura sugerida:

```
README.md              como compilar, rodar cada algoritmo, rodar os testes; seção "Uso de IA"
algoritmo1/            código da dupla 1
algoritmo2/            código da dupla 2
comum/                 interface comum, leitura de dados, escrita de saída
datasets/              geradores ou instruções de download (não versionar arquivos enormes)
scripts/               scripts de teste de escalabilidade e de gráficos
resultados/            CSVs de tempos e os gráficos gerados
relatorio/             (opcional) fontes do relatório
```

**README** (a professora vai **executar** a partir dele):
- [ ] requisitos (versão do JDK, ou do Spark e Scala);
- [ ] como compilar;
- [ ] como rodar cada algoritmo, com um exemplo de comando;
- [ ] como obter ou gerar os datasets;
- [ ] como rodar o teste de escalabilidade e gerar os gráficos;
- [ ] **Uso de IA:** se foi usada, para quê e **em quais arquivos** (lista explícita);
- [ ] integrantes e a dupla responsável por cada algoritmo.

**Histórico de commits:** pequeno, frequente e de quem escreveu. Cada integrante faz commits do
próprio trabalho. Evitar um commit grande no fim.

---

## 10. Ligações com a teoria (o que justificar no relatório)

O relatório pede *"as decisões tomadas para o paralelismo/distribuição"* e *"qual foi a ideia de
particionamento, por que vocês tomaram aquela decisão"*. A teoria da Prova 2 dá o vocabulário:

| Decisão a justificar | Tópico | Pergunta que o relatório deve responder |
|---|---|---|
| Decomposição de dados ou de tarefas | [25](25_decomposicao_tarefas_dados.md) | O que foi dividido e por quê? |
| Dependências e sincronização | [26](26_analise_dependencias.md) | Onde as partes precisam esperar umas pelas outras? |
| Padrão de algoritmo | [27](27_padroes_estrutura_algoritmo.md) | Qual dos 6 padrões, e por que a árvore de decisão leva a ele? |
| Granularidade (tamanho de cada tarefa) | [28](28_tasks_threads_granularidade.md) | Quantas tarefas e de que tamanho? Testaram outros tamanhos? |
| Particionamento e balanceamento | [29](29_balanceamento_carga_particionamento.md) | Bloco, cíclico, aleatório, espacial? Estático ou dinâmico? |
| Localidade de dados, mestre-escravo | [30](30_mapeamento_dinamico_localidade_dados.md) | Quem coordena e quem trabalha? Os dados ficam perto de quem os processa? |
| Ferramenta Java | [31](31_executorservice_callable_future.md), [32](32_fork_join_work_stealing.md) | Por que ExecutorService ou Fork/Join? |
| Ferramenta distribuída | [33](33_big_data_hadoop.md)–[35](35_mapreduce.md) | Onde ocorre shuffle? Quanto dado se move? |
| Métricas | [02](02_metricas_desempenho_speedup.md) | Tempo, speedup, eficiência, lei de Amdahl (parte sequencial) |

---

## 11. Relatório e apresentação

**Relatório (até 12/11, template no Classroom):**
1. **Algoritmos:** explicação geral de cada um.
2. **Implementação e paralelismo:** como foi implementado; **a ideia de particionamento e por que**
   (§10).
3. **Experimentos:** máquina (CPU, núcleos, RAM, SO), versões, datasets (origem, distribuição,
   tamanhos), parâmetros, metodologia de medição (§8).
4. **Resultados:** os **2 gráficos** e, se útil, uma tabela com os tempos.
5. **Discussão:** o que escalou bem, o que não, e por quê. Exemplo da fala: *"Tomamos essa
   decisão, usamos esse tipo de paralelismo, mas não funcionou tão bem quanto este outro."*
6. **Melhorias e trabalhos futuros.**
7. **Uso de IA** (obrigatório): para quê e em quais arquivos.

**Apresentação (13/11):** ~20 min por grupo, ~5 min por pessoa. Algoritmos, decisões de projeto e
resultados: o conteúdo do relatório. A divisão de quem fala o quê deve ser **justa**.

---

## 12. Plano sugerido (a partir de 10/10)

| Período | Meta |
|---|---|
| **10–18/10** | Fechar o grupo, a tarefa, os algoritmos e a linguagem; checar se a combinação está livre; **criar o repositório** |
| **até 25/10** | **Enviar o formulário.** Combinar a interface comum (§7); a leitura de dados e os datasets prontos; o primeiro commit de cada dupla |
| **26/10–2/11** | Duplas implementam e validam a versão paralela com dados pequenos; scripts de teste prontos |
| **3–7/11** | Rodar os testes completos **na mesma máquina**; gerar os gráficos; refazer o que for estranho |
| **8–12/11** | Escrever o relatório e montar os slides; **entregar até 12/11** (de preferência no dia 11) |
| **13/11** | Apresentação |

Folga: os testes completos podem demorar (algoritmos O(n²) com 150 mil pontos). Rodar um teste
**cedo**, com o maior n, para descobrir o tempo real.

---

## 13. Decisões em aberto (preencher na sessão)

| Decisão | Valor |
|---|---|
| Integrantes do grupo | |
| Tamanho do grupo (4 ou 3) | |
| Tarefa | |
| Algoritmo 1 / dupla | |
| Algoritmo 2 / dupla (ou decomposição 2, no grupo de 3) | |
| Linguagem e framework | |
| Dataset 1 (origem, distribuição, tamanho) | |
| Dataset 2 (origem, distribuição, tamanho) | |
| Máquina dos testes (CPU, núcleos, RAM) | |
| Link do repositório | |
| Combinação confirmada como livre? | |

---

## 14. Pontos de atenção

1. **IA não pode implementar o algoritmo paralelo/distribuído.** Só leitura e escrita de arquivos,
   scripts de teste e de gráficos. **Declarar tudo** no README e no relatório.
2. **Repositório criado antes de 25/10**, com commits progressivos. A professora acompanha.
3. **Mesma linguagem e framework** para os dois algoritmos do grupo.
4. **Mesma interface e mesmo formato de saída**: combinar antes de dividir o trabalho.
5. **2 datasets distintos**, de 10 mil a 150 mil pontos; sintéticos com distribuições diferentes.
6. **Medir só o algoritmo:** fora a leitura do arquivo e a inicialização do framework.
7. **Todos os testes na mesma máquina**, com os mesmos parâmetros e threads; repetir e usar a
   mediana.
8. **2 gráficos** (um por dataset), **2 linhas** cada, X = tamanho e Y = tempo.
9. **O relatório discute o porquê**, com a teoria (§10), não só os números.
10. **Combinação de algoritmos exclusiva por ordem de registro**: decidir e registrar cedo.
11. **Prazos:** formulário 25/10 · relatório 12/11 · apresentação 13/11 · P2 27/11.
