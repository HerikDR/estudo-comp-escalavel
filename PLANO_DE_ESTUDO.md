# Plano de construção do material de estudo — Computação Escalável (2COP022)

> Disciplina: Computação Escalável · Profa. Larissa C. Shimomura · 6º semestre, Ciência de Dados e IA
> Status: **Aulas 06–08 concluídas** (13/10/2026) · **Aulas 09, 09b (Scala), 10, 11 e 11a (complementar) concluídas** (13/11/2026) · **Aula 12 concluída** (14/11/2026): `estudo/12_joins_spark_revisao_prova.md`.
> **P2 em 27/11/2026** — 14 dias a partir desta atualização.

---

## 1. Objetivo e escopo

Produzir, para cada aula, um arquivo `.md` de estudo autocontido com **teoria + pontos-chave de implementação em Java**, diagramas **Mermaid** e uma **conclusão de revisão** ao final. O foco é **estritamente o que foi dado em aula**: os livros e a internet entram para *aprofundar e explicar melhor* os tópicos dos slides, não para ampliar o escopo.

### 1.1 Como o código será tratado (critério da prova)

A professora **não cobra escrever Java do zero**. As questões de código serão do tipo:

| Tipo de questão | O que o material precisa treinar |
|---|---|
| **"Onde utilizar a função/classe X?"** | Para cada construção Java (`submit`, `Callable`, `Future.get`, `fork`, `join`, `compute`, `invoke`, `shutdown`…): *quando usar, quando não usar, o que a diferencia das vizinhas* |
| **"Interprete o código"** | Código curto lido linha a linha: o que faz, em qual ordem executa, quantas threads, o que imprime, onde bloqueia |
| **"Verifique se o código está correto"** | Catálogo de **erros típicos** (código com bug propositalmente) e como reconhecê-los |

**A partir da Aula 09 o código muda de linguagem:** passa a ser **Scala** (coleções, funções de ordem superior) e **Spark** (RDD, DataFrame, SparkSQL). O mesmo critério vale — "onde usar `map`/`flatMap`/`reduceByKey`?", "o que este pipeline imprime?", "este código está correto?". A Aula 11 traz um quarto tipo de questão, **sem código**: desenhar e otimizar **árvores de consulta** em álgebra relacional e calcular **seletividade/cardinalidade**.

Consequência para o material: **sem exercícios de "escreva um programa que…"** e sem projeto Java para o aluno montar. O código aparece como **trechos curtos e anotados** focados nos pontos importantes de implementação. Os exercícios propostos em aula (soma com `Callable`, soma com ForkJoin, matriz, árvore) entram **como código resolvido para leitura e interpretação**, não como desafio de codificação.

**Fora de escopo (a menos que você peça):** OpenMP, MPI, sincronização de baixo nível (`synchronized`, locks, `wait/notify`), segurança, consenso/Paxos — estão nos livros, mas não nas aulas. Das aulas novas, também ficam fora: implantação em Kubernetes/YARN, configuração detalhada de Docker, estruturas de índice (árvore B+) e fórmulas de custo por **acesso a blocos** (o slide "Escopo de aula" da Aula 11 exclui isso explicitamente), e estimativa de cardinalidade com *machine learning* (só citada, sem detalhamento).

## 2. Inventário do que foi lido

| Fonte | Conteúdo | Observação |
|---|---|---|
| `fontes/aulas/Aula6_1209.pdf` | 44 slides — 12/09 | Quase todo texto; 2 slides com figura (p.25, p.41) |
| `fontes/aulas/Aula7_2509.pdf` | 59 slides — 25/09 | Slides 44, 48, 50–51 têm **código em imagem** (já inspecionados) |
| `fontes/aulas/aula8-0910.pdf` | 39 slides — 09/10 | Slides 6–7 (código ForkJoin) e 29 (word count) são imagem; slides 33–39 = Trabalho do Semestre (**fora do material**, por decisão do aluno) |
| `fontes/códigos/ExemploExecutorService/Main.java` | `Runnable`, `Callable`, `FixedThreadPool(3)`, `Future.get()` | Usa `RandomGenerator` → exige Java 17+ |
| `fontes/códigos/ExemploExecutorService/BuscaMaiorValorTask.java` | `RecursiveTask` + `ForkJoinPool.commonPool()` | Código real usa `LIMIAR = 5_000_000`; o slide mostra `500` |
| `fontes/aulas/aula9_2010.pdf` | 56 slides — 20/10 | Limitações do MapReduce, NoSQL, CAP/BASE/ACID, programação funcional, Scala, Spark (arquitetura, RDD, DAG, *lineage*, *lazy evaluation*, Pair RDDs, shuffle). Figuras de RDD/shuffle sem texto |
| `fontes/aulas/aula_programacao_scala_2110.pdf` | 22 slides — 21/10 | Slides 1–18 **repetem** a parte funcional/Scala da Aula 9; novos: instalação (p.19) e **4 exercícios** (p.20–22) |
| `fontes/aulas/aula10_3010_spark_introducao_a_processamento.pdf` | 24 slides — 30/10 | Capa diz "Aula 9" (erro do slide). p.1–11: modos de execução, SparkSession, RDD × DataFrame × Dataset, SparkSQL/Catalyst. p.12–24: introdução a processamento de consultas — **repetida** no início da Aula 11 |
| `fontes/aulas/aula_otimizacao_de_consulta_11_0611.pdf` | 83 slides — 06/11 | Processamento e otimização de consultas: árvore de consulta, equivalências algébricas, heurísticas (*push-down*), custo, seletividade, algoritmos de junção, cardinalidade, *data skew*, histogramas. Muitas árvores e tabelas em figura |
| `fontes/aulas/Introducao_Algebra_Relacional.pdf` | 45 slides — **sem data** no PDF | **Aula complementar** (atualização 14/11: "introdução a álgebra relacional" está na lista de tópicos da P2): σ, π, ρ, ×, ∪/∩/−, junção (θ, equi, natural), divisão, agrupamento, precedência, árvore de operações, resumo com SQL. 4 exercícios com resposta no slide. É o pré-requisito de vocabulário da Aula 11 ("Lembrando… operadores da álgebra relacional") |
| `fontes/códigos/ExemploExecutorService/CodigosScala.ipynb` | 39 células, kernel Scala (Almond) | `val`/`var`, `if`/`for`, `def`, lambdas e `_`, `class`/`case class`, `Option`, `trait`, coleções mutáveis × imutáveis, `map`/`flatMap`/`reduce`/`groupBy`/`filter`, **WordCount** com coleções |
| `fontes/códigos/ExemploExecutorService/exemplo_spark.ipynb` | Notebook Spark (Scala, `import $ivy`) | RDD: `parallelize`, `map`, `filter`, `reduceByKey`, `repartition`/`coalesce`, `partitionBy` com particionador próprio; SparkSQL: `read.csv`, `printSchema`, `select`/`withColumn`/`groupBy`/`agg`, `createOrReplaceTempView` + `sql`, `explain`, tipos de **join**, escrita com `partitionBy` |
| `fontes/códigos/ExemploExecutorService/AulaSparkDemo/` | Projeto sbt (Scala 2.12.18, Spark 3.5.0) | `ExemploSparkMain.scala`: `SparkSession` `local[2]`, `toDF`, `show`, `filter` |
| `fontes/códigos/ExemploExecutorService/spark_docker/` | Dockerfile + `docker-compose.yml` + `spark_submit.sh` | Cluster **Standalone** simulado: 1 master + 2 workers (2 cores/2 GB cada) e `spark-submit` do `.jar`. Pastas `__MACOSX`/`.DS_Store` são lixo do zip |
| `fontes/livros/Patterns for Parallel Programming.pdf` | Mattson, Sanders & Massingill | Base teórica das aulas 6–7 |
| `fontes/livros/Distributed_Systems_4.pdf` | van Steen & Tanenbaum, 4ª ed. | Apoio para threads, servidores, escalabilidade, balanceamento |

**Ambiente verificado:** Java 24.0.2 (`javac` e `java` disponíveis) — todo código Java do material poderá ser compilado e executado antes de entrar nos arquivos.

**Ambiente para Scala/Spark (verificado em 13/11):** `sbt` instalado (`C:\Program Files (x86)\sbt`); **sem** `scala` CLI, sem `spark-submit`, sem Docker. O JDK padrão (24) **não é suportado** pelo Spark 3.5 (aceita 8/11/17); há um **JDK 17** em `~/.jdks/ms-17.0.16`, que será usado para rodar o projeto sbt (`AulaSparkDemo`) em modo `local[2]`. Riscos: no Windows, escrita de arquivos pelo Spark (`write.csv`) costuma exigir `winutils.exe`/`HADOOP_HOME` — se falhar, o trecho é marcado como *não executado localmente*.

### 2.1 Pontos de atenção encontrados nas aulas

- **Slide × código:** `LIMIAR = 500` (slide, Aula 8 p.6) vs. `5_000_000` (arquivo do professor). Explicar no material que o limiar é um parâmetro de granularidade e por que o valor depende do custo por elemento.
- **Slide Aula 7 p.9:** a discussão "1 thread por partição *vs.* 1 por tarefa×partição" não tem resposta fechada ("teria que testar"). O material deve registrar o raciocínio, não inventar um veredito.
- **Slide Aula 7 p.21–22 (balanceamento dinâmico):** os custos individuais de P1…P8 não são dados, só as somas por thread. Ao reproduzir o exemplo, deixar explícito qual suposição foi feita.
- **Livro × Java moderno:** o Apêndice C do *Patterns* é de ~Java 5 (pré-`ForkJoinPool`). Para `ExecutorService`/`ForkJoin`/`Callable` a referência primária será a **documentação oficial do JDK**, não o livro.
- **Lacuna nos livros:** *Distributed Systems* **não menciona** MapReduce, Hadoop, HDFS nem Spark (busca textual sem resultados). A Aula 8 (parte Big Data) depende de **fontes da internet**.

**Aulas 09–11:**

- **Conteúdo duplicado:** o PDF de Scala (21/10) repete a parte de programação funcional e Scala da Aula 9, e os slides 12–24 da Aula 10 reaparecem no início da Aula 11. Cada assunto entra **uma vez só**, no arquivo da aula em que é aprofundado, e os outros arquivos apenas apontam para ele.
- **Numeração dos PDFs:** a capa da Aula 10 diz "Aula 9", e a aula de Scala não tem número. O material segue as **datas**: 09 (20/10), 09b (21/10), 10 (30/10), 11 (06/11).
- **Aula 9 × Aula 8 (B5):** arquitetura do Spark, RDD e Hadoop × Spark já aparecem em `08_…md` §B5. O `09` **aprofunda** esses tópicos sem copiar o texto: retoma em 2–3 linhas e linka para B5.
- **"Teorema BASE" (Aula 9):** BASE é um **modelo/filosofia** de consistência, não um teorema (o teorema é o CAP). Registrar isso sem contradizer a professora de forma agressiva.
- **`Function1[R, A]` (Aula 9 / Scala p.17):** o slide escreve os parâmetros na ordem `[R, A]`. Na biblioteca real é `Function1[-T1, +R]` (entrada primeiro). O slide é coerente consigo mesmo; explicar a diferença em nota.
- **Notebook `exemplo_spark.ipynb`:** `.master("local[2]")` vem com o comentário "utiliza todas as threads da CPU" — errado (`local[2]` = 2 threads; todas = `local[*]`). Bom candidato a "este código está correto?".
- **Notebook `CodigosScala.ipynb`:** `val quadrado = _ * 2` na verdade **dobra** (e `quadrado(2)` imprime 4 por coincidência). Mesmo uso como questão de leitura.
- **Notebook × slides sobre Java:** o notebook diz "Java 8 ou 11"; a documentação do Spark 3.5 também aceita 17. Confirmar na doc oficial antes de afirmar.
- **Cardinalidade uniforme (Aula 11):** 1000 × 1/3 = 333,3; o slide arredonda para **334**. Explicar que é estimativa (arredondamento para cima), não erro de conta.
- **Exercício de heurísticas (Aula 11):** o esquema `Funcionario/Trabalha_em/Projeto` (consulta "Aquarius") é o exemplo clássico de Elmasri & Navathe, *Sistemas de Banco de Dados*. O livro **não está em `fontes/`**: a solução segue os slides passo a passo, e a referência ao livro entra só como "Para ir além".
- **Álgebra relacional (PDF sem data):** o arquivo não traz data nem número de aula; foi tratado como **complementar** e posicionado como pré-requisito da Aula 11 (`11a`). Se a data real for outra, só o nome do arquivo muda.
- **Termos que variam no mesmo PDF:** "subtração" (slide de união/interseção) × "diferença" (sumário); "Cursos(id, …)" × "Cursos(codigo_curso, …)" — a segunda forma é usada só para a **junção natural**, que exige atributo de mesmo nome. Padronizar no texto e avisar.
- **Exercício 2 (união):** `Aluno` e `Professor` têm o mesmo esquema (matricula, nome, codigo_curso), logo já são união-compatíveis; a resposta do slide projeta `nome` em cada uma **antes** da união (`π nome(Aluno) ∪ π nome(Professor)`). Explicar por que projetar antes: a pergunta pede só nomes, e, como a álgebra é baseada em conjuntos, nomes repetidos entre alunos e professores aparecem uma vez.
- **Slide "Projection push-down — é possível aqui?" (Aula 11):** a pergunta fica em aberto no slide. O material responde com o raciocínio (quais colunas precisam subir até a junção), não só com "sim/não".

## 3. Estrutura de pastas proposta

```
Computação Escalavél/
├── PLANO_DE_ESTUDO.md                  ← este arquivo
├── fontes/                             ← material original (não alterar)
└── estudo/
    ├── 06_padroes_de_paralelismo.md
    ├── 07_threads_balanceamento_executor.md
    ├── 08_forkjoin_bigdata_hadoop.md
    ├── 09_nosql_cap_spark_rdd.md                 ← Aula 9  (20/10)
    ├── 09b_programacao_scala.md                  ← Aula de Scala (21/10)
    ├── 10_spark_pratica_sparksql.md              ← Aula 10 (30/10)
    ├── 11a_introducao_algebra_relacional.md      ← COMPLEMENTAR, apoio à Aula 11 (PDF sem data)
    └── 11_processamento_otimizacao_consultas.md  ← Aula 11 (06/11)
```

- Numeração = número da aula (`06`, `07`, `08`, …), como solicitado.
- Todo código fica **inline** no `.md` (estudo sem trocar de arquivo). Não haverá pasta de projeto Java: os trechos são validados em pasta temporária, fora do material, só para garantir que estão corretos.
- A aula de Scala (21/10) não tem número no PDF e cai na mesma semana da Aula 9 ("Essa semana: … Programação em Scala"), por isso `09b`.
- Aulas futuras (12 em diante) seguem o mesmo padrão; ver §8.

## 4. Template padrão de cada arquivo

Todos os arquivos seguem a mesma ordem, para facilitar a revisão:

1. **Cabeçalho** — número/data da aula, tópicos, pré-requisitos, tempo estimado de estudo.
2. **Mapa da aula** — um diagrama Mermaid mostrando como os tópicos se conectam.
3. **Teoria** — seções na ordem dos slides, cada uma com: definição → por que importa → exemplo → armadilhas.
4. **Código Java — pontos de implementação** — trechos curtos (não programas completos), cada um com:
   - **O que faz** (interpretação linha a linha, anotada);
   - **Onde usar** (e onde *não* usar);
   - **Saída/comportamento esperado** (quantas threads, ordem, onde bloqueia);
   - **Tabela comparativa** quando há construções parecidas (ex.: `Runnable` × `Callable`, `fork` × `compute` × `invoke`).
5. **Treino de leitura de código** — por aula: questões "interprete o código", "onde usar X?" e **"este código está correto?"** (código com bug + diagnóstico + correção), todas com gabarito. Os "Para discutir" e os exercícios da aula entram aqui como **solução comentada para leitura**.
6. **Erros comuns / o que cai em prova** — a P2 é em **27/11**. Inclui o catálogo de bugs típicos da aula (ex.: `get()` dentro do loop de `submit`, esquecer `shutdown()`, `fork()` sem `join()`).
7. **Fontes** — livro (capítulo e página) e links oficiais.
8. **Conclusão para revisão** *(obrigatória, sempre no final)*:
   - Resumo em tópicos (≤ 10 itens);
   - Tabela de conceitos-chave (termo → definição de uma linha → quando usar);
   - Perguntas de autoavaliação **com respostas**;
   - Checklist "sei explicar / sei implementar";
   - Ligação com a próxima aula.

**Adaptações a partir da Aula 09:** no item 4, "Código Java" vira **"Código Scala/Spark"** (09b e 10) ou **"Álgebra relacional e SQL"** (11), com a mesma estrutura (o que faz / onde usar / saída esperada / tabela comparativa). Na Aula 11, as árvores de consulta são desenhadas em Mermaid (`flowchart TD`, com π, σ, ⋈ e × nos nós) e as tabelas de exemplo (Aluno/Departamento) são reproduzidas para mostrar as tuplas que sobram em cada passo.

**Convenções:** português; termos técnicos mantidos em inglês na primeira ocorrência com tradução (*work stealing* — "roubo de trabalho"); código Java com comentários em português, no estilo da professora; todo diagrama Mermaid acompanhado de uma frase dizendo o que observar nele.

## 5. Plano por aula

### 5.1 Aula 06 — 12/09 · Identificando paralelismo e escolhendo a estrutura do algoritmo

**Natureza:** aula essencialmente conceitual (sem código nos slides). O "código" será ilustrativo e curto.

| Seção | Conteúdo (slides) | Diagrama Mermaid planejado |
|---|---|---|
| 1. Visão geral | Memória compartilhada × passagem de mensagens; SIMD × MIMD e qual padrão favorecem; 4 passos para paralelizar (identificação → estrutura do algoritmo → estruturas de suporte → mecanismos de implementação) | `flowchart` dos 4 espaços de projeto do livro |
| 2. Decomposição | Tarefas × dados; exemplo 1 mi de registros (média/min/max/desvio padrão) nas 3 variantes: só dados, só tarefas, **ambas**; critérios de boa decomposição (flexibilidade, eficiência, simplicidade) | Árvore de decomposição do exemplo (3 variantes) |
| 3. Análise de dependências | Dependência por dados e por ordem; *Group Tasks*, *Order Tasks* (T1,T2 → T3), *Data Sharing*, **grafo de dependência de tarefas**, *Design Evaluation* | `flowchart` do grafo T1/T2→T3 e do A/B/C/D; checklist do *Design Evaluation* |
| 4. Estrutura do algoritmo | Árvore de decisão (tarefas / dados / fluxo de dados) → *Task Parallelism* (embaraçosamente paralelo e dependências isoladas), *Divide and Conquer* (dividir→resolver→combinar; custo em memória distribuída), *Geometric Decomposition* (comunicação, balanceamento, granularidade), *Recursive Data*, *Pipeline* (gargalo no estágio mais lento), *Event-Based Coordination* | Reconstrução da árvore de decisão (Aula 6 p.41); árvore de D&C; grade geométrica com fronteiras; pipeline com buffers; eventos |
| 5. Discussões da aula | 4 problemas de decomposição (ler 4 arquivos; requisição web; maior valor; ordenar); paralelizar **KNN**; **ensemble** de classificadores; **consulta SQL com JOIN** de subqueries | Grafo de tarefas para a query SQL; esquema do KNN paralelo e do ensemble |

**Código Java (apenas pontos de implementação, a aula não tem código):**
- Como se calculam `inicio`/`fim` de cada *chunk* a partir de um tamanho de bloco (e o erro clássico do último bloco/resto da divisão) — liga com "flexibilidade" e prepara a Aula 7;
- Leitura de um laço "média + mín + máx em uma passada": por que isso é um bom candidato a decomposição de dados;
- *Treino de leitura:* dado um trecho/pseudocódigo, identificar **qual padrão** (task parallelism, D&C, pipeline…) e **onde estão as dependências**.

**Referências:** *Patterns* cap. 3 (PDF p.39–71), cap. 4 (p.72–135, árvore de decisão em §4.2.3, p.75); cap. 2 §2.2 (Flynn/MIMD, p.23–27).
**Fonte externa citada no slide:** Stanford CS149 (gfxcourses.stanford.edu/cs149) — consultar para a figura-guia.

### 5.2 Aula 07 — 25/09 · De decomposição para threads: granularidade, balanceamento, localidade e `ExecutorService`

**Natureza:** maior carga de conceito + primeiros códigos Java.

| Seção | Conteúdo (slides) | Diagrama Mermaid planejado |
|---|---|---|
| 1. Decomposição → threads | Mesmo exemplo min/máx/média: 4 threads (1/partição) × 12 threads (3/partição). Trade-off: passar 1× vs 3× pelos dados | `flowchart` comparando as duas estratégias |
| 2. Task × Thread × Chunk | Definições; 4 cores / 4 threads / 32 chunks | Fila de 32 chunks consumida por 4 threads |
| 3. Granularidade e nº de threads/tasks | Tasks grandes × pequenas; CPU-bound × I/O-bound; custo de criar/trocar contexto; "o objetivo não é maximizar threads" | Gráfico conceitual (`xychart-beta`) tempo × granularidade |
| 4. Balanceamento **estático** | Carga heterogênea, thread mais lenta dita o tempo total; quando usar | `gantt` com 4 threads e 1 partição pesada |
| 5. Balanceamento **dinâmico** | Exemplo 8 tarefas/4 threads (20/20/100/100 ms); vantagens e custos | `gantt` estático × dinâmico (mesmo exemplo, tempos calculados) |
| 6. Mapeamento estático | Blocos (1D/2D), **blocos cíclicos** (round robin), blocos aleatórios, particionamento de grafos, mapeamento por tarefa/hierárquico | Vetor de 16 posições distribuído em bloco × cíclico × aleatório (tabela + `flowchart`) |
| 7. Mapeamento dinâmico | Centralizado (**Master/Slave**, gargalo) e distribuído; *load balancer* em sistemas distribuídos: **Round Robin** (estático) × **Least Connection** (dinâmico) | `sequenceDiagram` master→workers; `flowchart` do load balancer |
| 8. Localidade de dados | Espacial × temporal; hierarquia de cache; levar computação aos dados | Diagrama da hierarquia de memória |
| 9. Threads em Java | `Thread`/`Runnable` (platform threads); por que 1000 threads é má ideia | — |
| 10. `ExecutorService` | Thread pool reutilizável + fila; `Fixed`, `Cached` (60 s ocioso), `Single`, `Scheduled` | `flowchart` tarefas→fila→pool |
| 11. `Callable` e `Future` | `Runnable` (void) × `Callable<V>` (retorna, lança exceção, não vai em `Thread`); `Future` = "senha da lanchonete"; `submit`, `get()` bloqueante, `get(tempo, unidade)`, `isDone`, `cancel` | `sequenceDiagram` main→executor→worker→Future |
| 12. `ForkJoin` (introdução) | Fork/Join, *work stealing*, `RecursiveTask` × `RecursiveAction` — **aprofundado na Aula 8** | — (diagrama fica na Aula 8) |
| 13. Exercício 1 | Soma de `[1,70,40,20,89,34,71,56,2,6,30,23]` com 3 threads + `Callable` | Divisão do vetor em 3 chunks |

**Código Java — pontos de implementação (interpretação e uso):**
- `Main.java` da professora **lido por inteiro**: `TaskRunnable` + `FixedThreadPool(3)` com 6 tarefas (por que os nomes das threads se repetem), `TaskCallable` + `List<Future<Integer>>`; por que o primeiro laço só **submete** e o segundo **espera** (e o que muda se o `get()` for chamado dentro do laço de submissão);
- Quadro "**onde usar**": `Thread` × `ExecutorService`; `Runnable` × `Callable`; `submit` × `execute`; `Future.get()` × `get(tempo, unidade)` × `isDone` × `cancel`; `shutdown()` (e `awaitTermination`);
- Escolha do pool por cenário: `Fixed` × `Cached` × `Single` × `Scheduled` (tabela cenário → pool);
- Dimensionamento: `Runtime.getRuntime().availableProcessors()` e quando passar de nº de cores (I/O-bound);
- **Exercício 1 (soma com 3 `Callable`s)** como solução para **leitura**: como o vetor é fatiado em `inicio`/`fim`, o que cada `call()` retorna, como os `Future`s são somados;
- **Treino de leitura / "está correto?"** (com gabarito): `Callable` passado a `new Thread(...)`; `submit` sem `shutdown`; `get()` logo após cada `submit` (serializa tudo); nº de threads = nº de elementos; partição que ignora o resto da divisão.

**Referências:** *Patterns* §5.5 Master/Worker (PDF p.158), §5.9 Shared Queue (p.198), §5.10 Distributed Array — bloco/cíclico (p.213–225, `cyclic` aparece em p.217–221), §6.2 UE Management (p.232), cap. 3 de *Distributed Systems* §3.1 Threads (p.128–142) e §3.4 Servers/Server clusters (p.165–183; *round robin* em p.180); JDK docs: `ExecutorService`, `Executors`, `Callable`, `Future`.

### 5.3 Aula 08 — 09/10 · `ForkJoin` em profundidade, Big Data, HDFS, MapReduce e Spark

**Natureza:** dois blocos distintos. Terá **sumário no topo** e separação clara entre os blocos A (Fork/Join) e B (Big Data).

> O conteúdo do **Trabalho do Semestre** (slides 33–39) **não entra** no material de estudo.

**Bloco A — Fork/Join (código):**

| Seção | Conteúdo | Diagrama Mermaid |
|---|---|---|
| A1. Conceito | Fork (dividir recursivamente até o caso base) / Join (combinar); *work stealing* | `flowchart` Tarefa→A..F→Join→Resultado (recria o slide p.4) |
| A2. Classes | `ForkJoinPool`, `ForkJoinTask`, `RecursiveTask<V>`, `RecursiveAction`, `compute()` | Diagrama de classes (`classDiagram`) |
| A3. Passo a passo de `BuscaMaiorValorTask` | Caso base → divisão → `fork()` na esquerda → `compute()` na direita → `join()` → `Math.max`; `commonPool()`; `invoke()` | Árvore de recursão para um vetor pequeno; `sequenceDiagram` fork/join |
| A4. Decisões de projeto | Limiar (500 × 5.000.000); por que `fork` de **um** lado e `compute` do outro; custo de tarefas pequenas demais; medir speedup | Gráfico conceitual limiar × tempo |
| A5. Exercícios | **Ex. 1** (soma do vetor com `ExecutorService`+`Callable` **e** com ForkJoin); **Ex. 2** (soma de matriz por blocos com ForkJoin; contar nós de uma árvore com Fork/Join) | Árvore de exemplo; blocos da matriz |

**Código Java — pontos de implementação (interpretação e uso):**
- `BuscaMaiorValorTask` (arquivo da professora) **interpretado linha a linha**: papel de cada campo (`array`, `inicio`, `fim`, `LIMIAR`), caso base, divisão, `fork()`/`compute()`/`join()`, `Math.max`, `commonPool()` e `invoke()` no `main`;
- Quadro "**onde usar**": `RecursiveTask<V>` × `RecursiveAction`; `fork()` × `compute()` × `join()` × `invoke()`; `ForkJoinPool` próprio × `commonPool()`; `ExecutorService` × `ForkJoin` (quando cada um serve: tarefas independentes e planas × recursivas/divide-e-conquista);
- Esqueleto reutilizável de um `RecursiveTask` (caso base → dividir → fork → compute → join → combinar) para reconhecer o padrão em variações: soma de vetor, soma de matriz por blocos, contagem de nós de árvore;
- Exercícios 1 e 2 da aula como **soluções para leitura**: o que muda de uma variação para outra (função de combinação, critério do caso base, como dividir uma matriz/árvore);
- **Treino de leitura / "está correto?"** (com gabarito): `fork()` nos dois lados e `join()` em ordem errada; esquecer o `join()`; caso base ausente (recursão infinita); limiar pequeno demais (overhead); `RecursiveAction` usado onde se precisa de retorno; `compute()` chamado no lugar de `invoke()` no `main`.

**Bloco B — Big Data, Hadoop/HDFS, MapReduce, Spark (teoria + simulação em Java):**

| Seção | Conteúdo | Diagrama Mermaid |
|---|---|---|
| B1. Big Data | Por que um nó só não basta; definição (Oracle); **5 Vs** | `mindmap` dos 5 Vs |
| B2. Hadoop | Commodity hardware, tolerância a falhas, *data locality*, shared-nothing; componentes | `flowchart` dos componentes |
| B3. HDFS | NameNode (mestre) × DataNode (escravo); blocos de 128 MB (exemplo 300 MB → 128+128+44); replicação (fator 3; regras de posicionamento); particionamento **lógico × físico**, Parquet | `flowchart` arquivo→blocos→DataNodes; `sequenceDiagram` leitura/escrita |
| B4. MapReduce | Map → shuffle/sort → Reduce; pares (chave, valor); reduce opcional; custo do shuffle e das gravações em disco; exemplo **word count** | `flowchart` recriando o word count do slide 29, passo a passo |
| B5. Spark | In-memory, RDD e tolerância a falhas, Driver/Executors, multilinguagem, módulos; Hadoop × Spark | `flowchart` Driver→Executors; tabela comparativa |

**Código Java — pontos de implementação:**
- Esqueleto do **WordCount da API Hadoop** (`Mapper`, `Reducer`, `Job`) **para leitura**: o que entra e sai do `map` e do `reduce` (tipos chave/valor), onde o shuffle acontece sem o programador escrevê-lo, o que é opcional (reduce) — rotulado como *referência, não executado localmente*;
- Mini-simulação de 10–15 linhas de Map → Shuffle → Reduce em Java puro, apenas como ajuda visual do fluxo do word count;
- Spark: apenas conceito nesta aula (a professora anuncia Scala e a API para as próximas). Nenhum código.

**Referências externas previstas (Bloco B):** documentação oficial Apache Hadoop (HDFS Architecture, MapReduce Tutorial) e Apache Spark (Overview, RDD Programming Guide); artigos originais *MapReduce* (Dean & Ghemawat, 2004) e *The Google File System* (2003); *Learning Spark* (O'Reilly) citado no slide 31. Todos serão **verificados e citados com link** na hora da escrita — nada de URL "de memória".

### 5.4 Aula 09 — 20/10 · Limites do MapReduce, NoSQL, CAP/BASE, programação funcional e funcionamento do Spark

**Natureza:** quase toda conceitual. Três blocos: (A) por que sair do Hadoop/relacional → NoSQL e CAP; (B) programação funcional como base do Spark; (C) o que acontece por dentro do Spark. Os slides de **sintaxe de Scala** (estrutura do programa, `val`/`var`, `class`/`trait`, "tudo é objeto") vão para o `09b`, para não duplicar.

| Seção | Conteúdo (slides) | Diagrama Mermaid planejado |
|---|---|---|
| A1. Recapitulando MapReduce | Vantagens (só 2 fases; grande escala) × desvantagens (nem todo algoritmo cabe em map/reduce; intermediários em disco; shuffle/sort pesa); HDFS é **WORM** (*write once, read many*); relacional escala mal na horizontal e tem esquema rígido | — (linka para `08` §B4) |
| A2. NoSQL | "Not only SQL"; 4 famílias com exemplo e aplicação: **chave-valor** (Redis, sessões), **documentos** (MongoDB, JSON/BSON), **grafos** (Neo4j/Neptune — Neo4j é ACID), **colunares** (Cassandra/HBase, consultas analíticas) | Tabela família → modelo → exemplo → uso; `flowchart` "qual família para qual problema" |
| A3. Teorema CAP | C, A, P; P é obrigatório → sistemas **CP** ou **AP**; exemplo da transferência bancária com nó falhando | Triângulo CAP (`flowchart`) e `sequenceDiagram` do exemplo do banco nos dois cenários (CP: erro/sem resposta; AP: saldo desatualizado) |
| A4. BASE × ACID | *Basically Available*, *Soft state*, *Eventually consistent*; ACID garante correção ao custo de disponibilidade, BASE o contrário | Tabela ACID × BASE |
| A5. Críticas | Stonebraker, "MapReduce: a major step backwards" (link do slide) | — (só referência) |
| B1. Programação funcional | Funções como valores (*first-class*), **imutabilidade**, **lambda**, **funções puras** (exemplo `calcularJuros` puro × impuro), **higher-order** (`map`, `reduce`, `criarMultiplicador`) | Esquema `numeros` → `map(x => x*2)` → `resultado` (entrada intacta) |
| B2. Por que funcional serve para distribuído | Imutabilidade (partições sem disputa), funções puras (recalcular dá o mesmo resultado → base da tolerância a falhas), HOF (o programador descreve, o framework distribui) | — |
| C1. Scala → Spark | Odersky/EPFL 2001; Zaharia criou o Spark em Scala; PySpark existe, mas algumas coisas só em Scala (ex.: regras no **Catalyst**) | — |
| C2. Arquitetura | **Driver** (`main`, `SparkContext`, agenda tarefas) × **Executors** × **Cluster Manager** — aprofunda `08` §B5.1 | `flowchart` Driver ↔ Cluster Manager ↔ Executors |
| C3. RDD e particionamento | RDD imutável, distribuído, API parecida com coleções Scala; partições seguem a camada de armazenamento (HDFS 300 MB → 128+128+44) ou particionamento **lógico** em memória; Driver diz "leia do byte X ao Y", Executor carrega na RAM | `flowchart` arquivo HDFS → blocos → partições → executors |
| C4. Transformações, DAG e *lineage* | Cada transformação gera um novo RDD e fica registrada no **DAG**; exemplo `textFile → filter("ERRO") → map(split)`; o *lineage* reconstrói só a partição perdida | DAG do exemplo dos logs (RDD1 → RDD2 → RDD3) |
| C5. *Lazy evaluation* | Nada executa até uma **ação** (`show`, escrita, `collect`…); permite otimizar o DAG; problema: ramos que reaproveitam um RDD são recalculados → `persist()`/`cache()` | `flowchart` com dois ramos saindo do mesmo RDD, com e sem `cache` |
| C6. Pair RDDs e shuffle | RDD de (chave, valor); métodos exclusivos (`reduceByKey`, `groupByKey`, `join`, `sortByKey`); particionar por chave (ex.: por mês); **shuffle** = redistribuir dados entre nós; ocorre em **transformações largas** (*wide*): `reduceByKey`, `groupByKey`, `aggregateByKey`, `join`/`cogroup` (sem co-particionamento), `distinct`, `sortByKey` | Estreita × larga: partições de entrada → saída, com setas cruzadas no caso largo |
| C7. Balanço do Spark | Vantagens (flexível, resolve em parte o desempenho do Hadoop) × desvantagens (gerenciar memória, curva de aprendizado, shuffle continua caro) | — |

**Código (leitura):** só os trechos de Scala/Spark que estão nos slides — `numeros.map(x => x * 2)` vs. o laço Java equivalente, `criarMultiplicador`, e o pipeline dos logs (`textFile`/`filter`/`map`/`show`). O treino de leitura cobra: **"em que linha o Spark começa a executar de fato?"**, **"esta operação causa shuffle?"** (lista de operações para classificar em estreita/larga), **"esta função é pura?"**, e **"CP ou AP?"** para cenários curtos.

**Para ir além (marcado):** artigo original do RDD (Zaharia et al., NSDI 2012 — o link `manosk/assets/papers/spark.pdf` do slide de recomendações), *Spark Internals* (japila.pl). Verificar os links antes de citar.

**Referências:** slides; *Distributed Systems* 4ª ed. — conferir se/onde discute CAP e consistência eventual (cap. 7, *Consistency and replication*) antes de citar; documentação oficial do Spark (RDD Programming Guide: transformações, ações, persistência, shuffle).

### 5.5 Aula de Scala — 21/10 · Programação em Scala (arquivo `09b`)

**Natureza:** aula prática. O teórico (p.1–18) é o mesmo da Aula 9; o material novo está no **notebook `CodigosScala.ipynb`** e nos **4 exercícios**.

| Seção | Conteúdo | Diagrama Mermaid |
|---|---|---|
| 1. Estrutura de um programa | `object` (singleton) + `def main(args: Array[String]): Unit`; `Unit` ≈ `void`; interpolação `s"Olá, $nome"`; JVM e interoperabilidade com Java | — |
| 2. Sintaxe essencial (notebook) | `val` × `var`; inferência de tipo; `if` como **expressão** (retorna valor); `for` com `to`, `until` e filtro (`if i % 2 == 0`); `def` sem `return` (última expressão é o retorno) | — |
| 3. Funções | Lambda (`x => x * 2`), tipo `Int => Int`, atalho `_`; funções como objetos (`Function1`, `apply`) | — |
| 4. Orientação a objetos | `class` × equivalente Java; **`case class`** (campos `val`, `equals`/`toString`/`copy` prontos); `trait` × `interface` | `classDiagram` trait/class |
| 5. `Option` no lugar de `null` | `Some`/`None`, `getOrElse` — por que existe | — |
| 6. Coleções | Imutáveis (padrão) × mutáveis (`scala.collection.mutable`); `:+` cria lista nova | — |
| 7. Funções de ordem superior | `map`, `flatMap`, `filter`, `reduce`, `groupBy`, `foreach`, `mapValues` — **tabela "entra → sai"** com exemplos de uma linha | `flowchart` do WordCount: frases → `flatMap(split)` → `map((_, 1))` → `groupBy` → soma |
| 8. WordCount com coleções | Exemplo do notebook, lido etapa por etapa, ligado ao word count do Hadoop (`08` §B4) | (mesmo do item 7) |

**Código Scala — pontos de implementação (interpretação e uso):**
- Quadro "**onde usar**": `map` × `flatMap` (o caso clássico de `split` que devolve lista de listas); `filter` × `collect`; `reduce` × `groupBy`+soma; `val` × `var`; `class` × `case class`; `Option` × `null`;
- Os **4 exercícios da aula como soluções para leitura**: (1) logs `ERROR` — `filter` + `startsWith` + extração da mensagem; (2) `case class Venda` + `groupBy` + soma + ordenação crescente por faturamento; (3) limpeza de `dadosBrutos` (linha vazia, sem vírgula, idade não numérica) com `case class` de usuário válido e `toIntOption`/`Try`; (4) "join" entre `Cliente` e `Pedido` por ID, descartando sem correspondência (= **inner join**, ponte para a Aula 11);
- **Treino de leitura / "está correto?"** (com gabarito): reatribuir `val`; `map` onde precisava de `flatMap` (resultado `List[Array[String]]`); `quadrado = _ * 2` (nome não bate com o que faz); `_` usado duas vezes na mesma lambda (`_ * _` ≠ `x => x * x`); `reduce` em lista vazia (exceção); `get` em `None`.

**Validação:** todos os trechos rodam via `sbt` (Scala 2.12.18, a mesma versão do `build.sbt` da professora) com JDK 17, em pasta temporária. Saídas copiadas da execução real.

**Referências:** notebook e slides; documentação oficial Scala (*Tour of Scala*, *Scala Book* — coleções, `case class`, `Option`). Conferir os links.

### 5.6 Aula 10 — 30/10 · Spark na prática: modos de execução, RDD × DataFrame × Dataset, SparkSQL

**Natureza:** prática. Base de código: `exemplo_spark.ipynb`, `ExemploSparkMain.scala` e `spark_docker/`. Os slides 12–24 (processamento de consultas) **vão para o `11`**.

| Seção | Conteúdo | Diagrama Mermaid |
|---|---|---|
| 1. Modos de execução | **Local** (núcleos da máquina, distribuição por threads — liga com a Aula 7), **Standalone** (o Spark gerencia o cluster; dá para simular em um PC — é o `spark_docker`), **Cluster** (Kubernetes/YARN; só citado) | `flowchart` dos 3 modos; `flowchart` do `docker-compose` (master + 2 workers, `spark-submit`) |
| 2. Ponto de entrada | `SparkContext` (RDDs) × **`SparkSession`** (desde o 2.0, contém o `SparkContext`); `builder().appName().master().getOrCreate()`; `local[2]` × `local[*]`; `spark.stop()` | — |
| 3. RDD na prática | `parallelize`, `map`, `filter`, `collect`, `reduceByKey`; `repartition` × `coalesce`; `partitionBy` com particionador próprio (função de particionamento do notebook) | Partições antes/depois de `repartition` e `coalesce` |
| 4. Três APIs | **RDD** (baixo nível, tipado, sem otimizador) × **DataFrame** (`Dataset[Row]`, esquema, Catalyst, erro de coluna só em execução) × **Dataset** (`case class`, tipado **e** otimizado, erro em compilação; só Scala/Java) | Tabela comparativa |
| 5. SparkSQL e Catalyst | API declarativa; transformações formam um **plano lógico** → Catalyst gera plano otimizado e escolhe as funções físicas; `explain()` para ver o plano | `flowchart` plano lógico → Catalyst → plano físico (antecipa a Aula 11) |
| 6. Funções principais | Leitura (`read.option("header",…).csv`, `printSchema`), transformações (`select`, `filter`, `withColumn`, `groupBy().agg()`, `orderBy`), **SQL direto** (`createOrReplaceTempView` + `spark.sql`), **tipos de join** (inner, cross, left/right/full outer, semi, anti), escrita (`write.mode().partitionBy("estado").csv`) | Tabela tipo de join → o que retorna, com o mesmo par de tabelinhas |
| 7. Exercício da aula | "Refaça os exercícios de Scala com RDD ou DataFrame" → os 4 exercícios do `09b` resolvidos **lado a lado**: coleções Scala × RDD × DataFrame | — |

**Código Scala/Spark — pontos de implementação (interpretação e uso):**
- `ExemploSparkMain.scala` **lido por inteiro**: `SparkSession`, `import spark.implicits._` (por que é necessário para `toDF` e `$"col"`), `toDF`, `show`, `filter($"Idade" > 23)`, `stop`; e o que muda quando roda via `spark-submit --master spark://spark-master:7077` no Docker;
- Quadro "**onde usar**": transformação × ação (`map`/`filter` × `collect`/`show`/`count`/`write`); `collect` em dado grande (traz tudo para o Driver); `repartition` (shuffle) × `coalesce` (só reduz, sem shuffle completo); `reduceByKey` × `groupByKey`; RDD × DataFrame × Dataset; API DataFrame × `spark.sql`; `cache`/`persist`;
- Ler a saída de `explain()` em nível básico: onde aparecem `Filter`, `Project`, o tipo de join e o `Exchange` (= shuffle);
- **Treino de leitura / "está correto?"** (com gabarito): `local[2]` com comentário "todas as threads"; sem nenhuma ação (nada executa); `collect()` seguido de processamento no Driver; `toDF` sem `import spark.implicits._` (não compila); `groupByKey().map(soma)` onde `reduceByKey` serve; coluna inexistente em DataFrame (erro em execução) × em Dataset (erro em compilação); RDD reaproveitado em duas ações sem `cache`.

**Validação:** rodar `AulaSparkDemo` com `sbt run` usando JDK 17 e reproduzir em um projeto sbt temporário as células do `exemplo_spark.ipynb` necessárias aos trechos do material (o notebook usa kernel Almond, que não está instalado). O cluster Docker **não** será executado (Docker ausente): a explicação do `docker-compose.yml` é por leitura e fica rotulada assim.

**Referências:** Spark docs oficiais — *Spark SQL, DataFrames and Datasets Guide*, *Submitting Applications* (`--master`, `local[K]`), *Spark Standalone Mode*, sintaxe de JOIN (link do notebook); *Learning Spark*. Verificar números de versão (3.5.0) e requisito de Java na própria doc.

### 5.7 Aula 11 — 06/11 · Processamento e otimização de consultas

**Natureza:** teoria + **exercícios de lápis e papel** (árvores de consulta, seletividade). É a aula mais próxima da P2, e a próxima aula anuncia "exercícios sobre o conteúdo para a prova". **Sem código Java/Scala**; o "código" é SQL, álgebra relacional e (como ponte) `explain()` do SparkSQL.

| Seção | Conteúdo (slides) | Diagrama Mermaid planejado |
|---|---|---|
| 1. Pipeline do processamento | Varredura/análise/validação → **otimizador** → gerador de código → processador; o que entra e sai de cada etapa | `flowchart LR` das 4 etapas com os artefatos (consulta → árvore → árvore otimizada com algoritmos → código) |
| 2. Varredura, análise, validação | Tokens; sintaxe (gramática); semântica (nomes existem no esquema?) | — |
| 3. Árvore de consulta | Folhas = relações, nós internos = operadores; execução **pós-ordem** (de baixo para cima, esquerda→direita); revisão de π, σ, ×, ⋈ | Árvore de `ALUNO ⋈ DEPARTAMENTO` com π no topo; árvore do exercício com `IDADE > 20` |
| 4. Equivalências algébricas | Cascata de σ, cascata de π, comutatividade de σ, de × e de ⋈ (**não** vale para outer joins) | Pares de árvores equivalentes lado a lado |
| 5. Otimização heurística | *Selection push-down*, *projection push-down*, trocar × + σ por ⋈; exemplo Aluno/Departamento com as tabelas (4 tuplas → 3 tuplas); a pergunta "é possível aqui?" respondida | Árvores antes/depois + tabelas intermediárias |
| 6. Algoritmo heurístico em 6 passos | Quebrar σ → descer σ → σ mais restritiva primeiro → × + σ = ⋈ → quebrar π → descer π | — |
| 7. Exercícios 1 e 2 (Aquarius) | Árvore inicial de `π Unome(σ…((Funcionario × Trabalha_em) × Projeto))` e **aplicação passo a passo dos 6 passos**, uma árvore por passo, como nos slides | **Uma árvore Mermaid por passo** (6–7 árvores) |
| 8. Otimização por custo | Busca no espaço de planos com função de custo **estimada**; componentes (acesso a disco, armazenamento intermediário, computação, memória, **comunicação** — relevante no distribuído); escopo da aula = custo de computação em nº de tuplas | — |
| 9. Seleção e seletividade | Busca linear; seletividade = tuplas que passam / total, entre 0 e 1; exemplos (PR: 200/1000 = 0,2; chave: 1/10000 = 0,0001); menor seletividade → relação intermediária menor | — |
| 10. Algoritmos de junção | **Nested loops** (custo \|R\|·\|S\|), **sort-merge** (ordenar R e S + varredura linear), **hash join** (*build* na menor + *probe*; \|R\|+\|S\|); quando usar cada um (poucas tuplas → NL; uma cabe na RAM → hash; grandes, não cabem → sort-merge) | `flowchart` build/probe do hash join; tabela algoritmo → custo → quando usar |
| 11. Fator de seleção de junção | Tuplas da junção / \|R × S\|; exemplo Clientes × Pedidos = 10000/(1000·10000) = 0,001 | — |
| 12. Estimativa de cardinalidade | Metadados do catálogo; hipótese de **distribuição uniforme** (1000 × 1/3 ≈ 334); **data skew** (60% SP) estraga a estimativa; **histogramas**; causas de erro (estatísticas desatualizadas, colunas correlacionadas, propagação em árvores grandes, funções no `WHERE`); tópicos recentes (ML, estruturas probabilísticas) só citados | Histograma uniforme × real (`xychart-beta`) |
| 13. Gerador de código e processador | Plano físico → instruções; interação com buffer, índices, armazenamento | — |
| 14. Ponte com o Spark | O mesmo pipeline no SparkSQL: **Catalyst** faz otimização por regras e custo; `explain()` mostra *push-down* e o algoritmo de junção escolhido; prepara a Aula 12 (broadcast/hash join, skew) | — |

**Treino (com gabarito):**
- **Desenhar a árvore** de 3–4 consultas SQL novas, no estilo dos slides (2 tabelas, depois 3);
- **Otimizar** uma árvore aplicando os 6 passos, justificando cada movimento pela equivalência usada;
- **"Esta transformação está correta?"**: comutar um `LEFT JOIN`; descer uma σ que usa colunas das duas tabelas para baixo de uma só; projeção que descarta a coluna da junção cedo demais; trocar × por ⋈ sem condição de junção;
- **Contas**: seletividade, fator de seleção de junção, cardinalidade estimada (uniforme × com skew) e escolha do algoritmo de junção para cenários dados.

**Validação:** contas conferidas à mão e por script; se for possível, rodar as consultas Aluno/Departamento no SparkSQL local (JDK 17) com `explain()` para mostrar o *push-down* real — rotulado como ilustração.

**Referências:** slides; tabela de operadores da álgebra relacional citada no slide (link `victorlimeira.github.io`); Elmasri & Navathe, *Sistemas de Banco de Dados* (capítulos de processamento/otimização de consultas) como "Para ir além" — **não está em `fontes/`**, citar sem número de página; doc do Spark (*Performance Tuning*, `explain`).

### 5.8 Introdução à Álgebra Relacional — **complementar** (arquivo `11a`)

**Natureza e peso:** aula de **apoio**; não cai diretamente na P2, mas fornece a notação que a Aula 11 usa o tempo todo (árvores de consulta, equivalências, *push-down*). Por isso o arquivo é **curto e enxuto** (≈ 250–350 linhas), sem treino extenso, e fica **depois** das aulas de prova na ordem de execução. Todas as tabelas do slide (Aluno/Cursos) são reaproveitadas, as mesmas da Aula 11.

| Seção | Conteúdo (slides) | Diagrama Mermaid planejado |
|---|---|---|
| 1. Consultas e linguagens | Consulta = "pergunta" ao banco; SQL (prática) × álgebra relacional e cálculo relacional (teóricas); recapitulação do modelo relacional (relação, tupla, atributo, esquema fixo) | — |
| 2. Propriedades | Operadores baseados em teoria dos conjuntos; **closure** ("relations in, relations out"); semântica de **conjunto** (sem tuplas duplicadas) — permite encadear operadores | `flowchart` de operadores encadeados |
| 3. Operadores unários | **Seleção σ** (filtro / partição horizontal; mesmo esquema), **Projeção π** (partição vertical; nº de tuplas ≤ entrada por causa do conjunto), **Renomeação ρ** (relação e atributo) | Tabela Aluno antes/depois de σ e π |
| 4. Produto cartesiano × | n+m atributos, \|R\|·\|S\| tuplas | Exemplo Aluno × Cursos (resumido) |
| 5. União, interseção, subtração | Exigem **união-compatibilidade** (mesmo nº de atributos, mesmos domínios); exemplo Aluno_1/Aluno_2 (6 tuplas na união, 1 na interseção, 3 na subtração) | Diagrama de Venn em tabela ou `flowchart` |
| 6. Junção | ⋈ com condição = × seguido de σ; **equijunção** (igualdade — tratamento especial, ver algoritmos na Aula 11); **junção natural** `*` (equijunção em todos os atributos em comum, que aparecem uma só vez) | Mesmo resultado por duas rotas (× + σ × ⋈) |
| 7. Divisão R/S | Consultas "**em todos**"; exemplo "alunos matriculados em todos os cursos da área Saúde"; requisitos (R tem mais atributos que S, ≥ 1 em comum) | Tabela R, S e R/S |
| 8. Operações estendidas | **Agrupamento** (𝒢, com `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`; exemplo de matrículas por curso), ordenação, outros tipos de junção (outer etc.; só citados) | Tabela antes/depois do agrupamento |
| 9. Precedência e parênteses | Ordem de precedência (do mais alto ao mais baixo, conforme slide); parênteses recomendados | — |
| 10. Árvore de operações | Folhas = relações; operadores executados quando os filhos estão prontos; base do processamento/otimização (**liga com a Aula 11 §3**) | Árvore `π nome,nome_curso(σ area='Saude'(Alunos ⋈ Cursos))` (slide) |
| 11. Mapa álgebra ↔ SQL | σ → `WHERE`; π → `SELECT`; × → `CROSS JOIN`; ⋈ → `JOIN … ON` ou lista no `FROM` + `WHERE`; consulta completa em álgebra e em SQL | Tabela de correspondência |

**Exercícios do slide (resolvidos para leitura):** (1) `π nome, matricula(σ idade>20 (Aluno))`; (2) união de `π nome(Aluno)` e `π nome(Professor)`, discutindo a união-compatibilidade; (3) junção Curso–Aluno com `σ área='Engenharia'` e projeção de `Aluno.nome, Curso.nome_curso`; (4) agrupamento por `codigo_curso` com `AVG(idade)` renomeado `media_idade`. Cada um também em **SQL**, como no sumário do slide.

**Mini-treino (3–5 itens, com gabarito):** ler uma expressão e dizer o que retorna; "isto é união-compatível?"; "σ ou π?" (qual operador responde a pergunta); qual consulta pede **divisão** ("em todos").

**Validação:** não há código. Conferir os resultados dos exemplos (contagem de tuplas de ×, união, interseção, subtração, junção, agrupamento) à mão; se possível, rodar a mesma consulta Aluno/Cursos em SQL (SQLite via Python, que já está instalado) para checar contagens.

**Referências:** slides (fonte única); tabela de operadores citada na Aula 11 (`victorlimeira.github.io`). Elmasri & Navathe só como "Para ir além" — fora de `fontes/`.

## 6. Estratégia de fontes e verificação

| Tipo de afirmação | Fonte primária | Verificação |
|---|---|---|
| Padrões de paralelismo, decomposição, Master/Worker, bloco/cíclico | *Patterns for Parallel Programming* (PDF p. indicadas; página impressa = PDF − 15) | Conferir o trecho antes de citar |
| Threads em SD, servidores, balanceamento, escalabilidade | *Distributed Systems* 4ª ed. | Idem |
| API Java (`ExecutorService`, `Callable`, `Future`, `ForkJoinPool`) | Javadoc oficial (JDK 24 instalado) | Compilar e executar |
| Hadoop/HDFS/MapReduce/Spark | Docs oficiais Apache + artigos originais | Abrir a página e confirmar números (ex.: 128 MB, fator 3) |
| Linguagem Scala (coleções, `case class`, `Option`) | Docs oficiais Scala (docs.scala-lang.org) | Rodar via `sbt` (Scala 2.12.18) |
| API Spark (RDD, DataFrame, SparkSQL, modos de execução) | Docs oficiais Spark **3.5** (a versão do `build.sbt`) | Rodar em `local[2]` com JDK 17; o que depender de cluster/Docker é rotulado |
| NoSQL, CAP, BASE | Slides + doc oficial de cada sistema citado (Redis, MongoDB, Neo4j, Cassandra) quando houver afirmação específica | Conferir antes de citar; nada "de memória" |
| Álgebra relacional | Slides (fonte única; complementar) | Conferir contagens de tuplas; SQLite para cross-check |
| Processamento/otimização de consultas | Slides (fonte principal); Elmasri & Navathe só como "Para ir além" (fora de `fontes/`) | Conferir contas (seletividade, fator de junção, cardinalidade) |
| Explicações dos slides | Slides da professora (prevalecem em caso de divergência) | Divergências listadas em §2.1 |

**Regras de qualidade:**
1. *(Aulas 06–08)* Todo trecho Java do material foi **compilado e executado em pasta temporária**. **A partir da Aula 09 (decisão do aluno em 13/11): o código é só didático e não precisa ser executado**; basta estar correto e coerente com a API. Os itens de ambiente/validação das §5.4–5.8 e do checklist deixam de ser obrigatórios. Os trechos com **bug proposital** são conferidos para que o erro seja de fato o descrito (compila-mas-falha, trava, resultado errado…). O que não for testável (Hadoop real) é rotulado explicitamente.
2. Benchmarks ficam em segundo plano (a prova é interpretativa); se aparecerem, são **exemplo da máquina local**, não valor universal.
3. Nenhuma afirmação do livro/web entra sem o trecho ter sido lido; citações como "(Patterns, PDF p.158)".
4. Conteúdo além dos slides vem marcado como **"Para ir além"** e nunca no corpo principal.
5. Mermaid: validar a sintaxe (renderização) antes de finalizar; manter diagramas pequenos.

## 7. Ordem de execução sugerida

| Etapa | Entrega | Dependências |
|---|---|---|
| 0 | Criar `estudo/` | — |
| 1 | **Aula 06** (teoria pura; base de vocabulário das demais) | Livro *Patterns* |
| 2 | **Aula 07** (conceitos + `Executor`/`Callable`/`Future`); validar os trechos e os "códigos com bug" em pasta temporária | Java 24 |
| 3 | **Aula 08, Bloco A** (ForkJoin + treino de leitura) | Etapa 2 |
| 4 | **Aula 08, Bloco B** (pesquisa web + Hadoop/HDFS/MapReduce/Spark) | Acesso à web |
| 5 | Revisão cruzada: links entre aulas, consistência de termos, checagem dos Mermaid, revisão dos gabaritos | Tudo |

Etapas 0–5 **concluídas** em 13/10/2026. Para as aulas novas:

| Etapa | Entrega | Dependências |
|---|---|---|
| 6 | Preparar ambiente: projeto sbt temporário com Scala 2.12.18 + Spark 3.5.0 rodando no JDK 17; testar `AulaSparkDemo` | `sbt`, `~/.jdks/ms-17.0.16` |
| 7 | **Aula 09** (`09_…`): NoSQL/CAP/BASE, programação funcional, Spark por dentro | Etapa 6 só para o pipeline dos logs |
| 8 | **Aula de Scala** (`09b_…`): notebook + 4 exercícios resolvidos + treino de leitura | Etapa 6 |
| 9 | **Aula 10** (`10_…`): Spark prático, 3 APIs, SparkSQL, exercícios refeitos em RDD/DataFrame | Etapas 6 e 8 |
| 10 | **Aula 11** (`11_…`): árvores de consulta, heurísticas (Aquarius passo a passo), custo, seletividade, junções, cardinalidade | — (pode ser feita em paralelo às etapas 7–9) |
| 11 | **Introdução à Álgebra Relacional** (`11a_…`, *complementar*): só depois das etapas 7–10; se sobrar tempo antes da P2, vale **antes** da 11 como aquecimento de notação | — |
| 12 | Ajustar a "ligação com a próxima aula" do `08` para apontar ao `09`; revisão cruzada 09–11 (termos, links, Mermaid, gabaritos) | Etapas 7–10 |

**Prioridade, se o tempo apertar antes da P2 (27/11):** 11 → 09 → 10 → 09b → 11a (complementar, última; mas se a notação da Aula 11 estiver pesada, leia o `11a` primeiro — são ~250 linhas). A Aula 11 é a mais "exercitável" em prova e não depende de ambiente; a 09b é a que mais se sobrepõe às outras (o treino de leitura de Scala também aparece no 10).

**Tamanho estimado:** 06 ≈ 300–400 linhas · 07 ≈ 500–700 · 08 ≈ 500–700 (2 blocos) · 09 ≈ 500–650 · 09b ≈ 350–450 · 10 ≈ 450–600 · 11 ≈ 600–800 (muitas árvores) · 11a ≈ 250–350.

## 8. Aulas futuras

**Aula 12 — concluída em 14/11** como `estudo/12_joins_spark_revisao_prova.md` (o PDF trouxe joins no Spark + exercícios de revisão; *data skew* não foi aprofundado nos slides). Planejamento original: **Aula 12** (anunciada no último slide da Aula 11): algoritmos de **Broadcast Join** e **Hash Join no Spark**, o problema do **data skew** no Spark, e **exercícios sobre o conteúdo para a prova**. Quando o PDF chegar: ler → nova seção §5.8 → arquivo `12_joins_distribuidos_data_skew.md`, com o template do §4. Os exercícios de revisão dessa aula entram resolvidos e, se cobrirem aulas anteriores, linkam para a seção correspondente.

A **apresentação do Trabalho** (13/11) não gera material, pela mesma decisão do §9.

## 9. Premissas assumidas (corrija se necessário)

- Os arquivos ficam em `estudo/` e o idioma é **português**.
- Você quer o material **enxuto e fiel às aulas**; extras ficam marcados.
- O Trabalho do Semestre **não** é discutido nos materiais (decisão do aluno).
- **Código = leitura, não escrita** (conforme §1.1): trechos curtos, "onde usar", interpretação e detecção de erros; sem exercícios de codificar do zero.
- Hadoop não será instalado; MapReduce será entendido via esqueleto de referência da API + mini-simulação ilustrativa.
- Slides só com imagem de código (Aula 7 p.44) foram tratados como introdução; o código completo vem do `Main.java` fornecido.
- A partir da Aula 09, o código de prova será **Scala/Spark** (não Java); o critério "leitura, não escrita" continua valendo.
- Docker não será instalado: o cluster Standalone (`spark_docker/`) é explicado lendo `docker-compose.yml` e `spark_submit.sh`, não executando.
- A aula de Scala (21/10) vira `09b` (mesma semana da Aula 9), e os conteúdos repetidos entre PDFs aparecem uma vez só (§2.1).

## 10. Checklist de aceite

- [x] 3 arquivos `.md` numerados (06, 07, 08), um por aula
- [x] Cada arquivo termina com **Conclusão para revisão** (resumo, tabela de conceitos, autoavaliação com respostas, checklist)
- [x] Todos os tópicos dos slides cobertos (rastreável pela tabela de cada seção do §5)
- [x] Cada aula com código tem: trechos interpretados linha a linha, quadro "onde usar", e treino de leitura com **"este código está correto?"** + gabarito
- [x] Todo trecho Java validado (compila/executa; bugs propositais conferidos) em pasta temporária
- [x] Mermaid em todas as seções marcadas, renderizando sem erro
- [x] Livros citados com página; links oficiais verificados
- [x] Divergências slide × código (§2.1) explicadas no texto

**Aulas 09–11:**

- [x] 5 arquivos `.md` (`09`, `09b`, `10`, `11`, mais o complementar `11a`), cada um terminando com **Conclusão para revisão**
- [x] Nenhum conteúdo duplicado entre 08/09/09b/10/11/11a: cada assunto em um só arquivo, os outros linkam para ele
- [x] Todos os tópicos dos slides cobertos (rastreável pelas tabelas §5.4–5.7)
- [x] `09b` e `10`: quadro "onde usar", trechos interpretados e "este código está correto?" com gabarito; os 4 exercícios de Scala resolvidos em coleções **e** em Spark
- [x] ~~Todo trecho Scala/Spark executado~~ — dispensado pelo aluno (código didático); só os trechos do `09` foram executados. As saídas citadas no `10` vêm das saídas registradas no notebook da professora
- [x] `11a` (complementar): operadores σ π ρ × ∪ ∩ − ⋈ ÷ e agrupamento com exemplo e SQL equivalente; 4 exercícios resolvidos; marcado como **"não cai diretamente na prova"** no cabeçalho; a Aula 11 linka para ele na revisão de operadores
- [x] `11`: árvores dos exercícios 1 e 2 desenhadas passo a passo; contas de seletividade/cardinalidade conferidas
- [x] Divergências das aulas novas (§2.1, bloco "Aulas 09–11") explicadas no texto
- [x] "Ligação com a próxima aula" do `08` atualizada para o `09`
