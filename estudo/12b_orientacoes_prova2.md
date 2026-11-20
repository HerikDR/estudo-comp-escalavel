# P2 — O que a Larissa disse sobre a prova (revisão da Aula 12)

> **Fonte:** transcrição automática da aula de revisão (`fontes/aulas/revisao.txt`). A transcrição tem muitos erros de reconhecimento de fala ("Radup" = Hadoop, "trads" = threads, "MEP" = map etc.); o conteúdo abaixo foi interpretado a partir do contexto. Quando a fala é ambígua, isso está indicado.
> **Situação:** a aula **não terminou**. A revisão cobriu **programação paralela**, **Big Data/Hadoop/HDFS/MapReduce** e **NoSQL/CAP/BASE**. Os exercícios de **Spark, SparkSQL, álgebra/otimização de consultas e algoritmos de join** ficaram para uma **aula extra** (ver §7).
> **Atualização:** a aula extra já aconteceu; o que ela disse está em [12c](12c_orientacoes_prova2_revisao2.md), que **prevalece onde houver conflito** (sem código na prova, exceto Spark; Spark/SparkSQL/otimização/joins deixam de ser "pendentes"; direção da seta do grafo confirmada como T1 → T3).
> **Prova:** P2, **27/11**. Material de cada tópico: [lista de tópicos com links](12_joins_spark_revisao_prova.md#b1-tópicos-da-prova).

---

## 1. Regras gerais da prova

| O que ela disse | O que significa para estudar |
|---|---|
| "Ainda não fiz a prova, mas com certeza vai estar **no mesmo nível**" dos exercícios da revisão e da lista | Os exercícios da [Aula 12, §B2](12_joins_spark_revisao_prova.md#b2-exercícios-de-revisão-resolvidos) são o **modelo** das questões |
| A troca "só muda os valores, né?" / "só muda o valor de nota" foi **brincadeira entre alunos** (você), não uma resposta dela | **Ela não disse que só mudam os valores.** O que ela disse foi "mais ou menos similar". Espere questões **parecidas** com as da revisão, não idênticas |
| "Vai ser **mais ou menos desse tamanho**. Tenho que considerar o tempo de vocês" (sobre o grafo de 6 tarefas) | Questões do tamanho dos exemplos; um aluno brincou com "30 tarefas" e ela negou |
| "**Não vou pedir para vocês programarem na prova.** Vocês já programaram bastante em papel" | Código aparece para **ler e interpretar** (ex.: quantas tarefas/threads, que decomposição), **não para escrever** |
| Programação paralela será cobrada de forma **mais teórica/conceitual**, "porque vocês já estão fazendo a parte prática através do trabalho" | Saber **definições**, **quando usar**, **exemplos** e **diferenças** entre conceitos |
| "Até a parte teórica, o que é conceito, qual a definição das coisas, **vocês precisam entender**" | Não basta decorar: entender o porquê |
| "**Não vou perguntar qual é a letra**" (sobre o significado de cada letra de BASE) | Saber a **ideia** de BASE, ACID e CAP; não precisa recitar a sigla letra por letra |
| Sobre respostas incompletas: "Coloquem **todas as coisas** [que podem executar em paralelo]. **Não vou tirar muitos pontos**, mas para deixar claro" | Respostas **completas e explícitas**, com as **condições** (ver §2.3) |

---

## 2. Programação paralela

### 2.1 O que ela vai cobrar

- **Decomposição de dados × de tarefas:** **definição** das duas, **quando** cada uma pode ser usada, **exemplo** de quando é adequada. ("Muitas vezes a gente pode aplicar os dois na prática.")
- **Estrutura do algoritmo**, **só** as organizações **por tarefas** e **por dados**:
  - por tarefas → **Task Parallelism** ("vergonhosamente/embaraçosamente paralelo": olhando o problema já se vê o que é paralelo) e **Divisão e Conquista** (dividir em subtarefas e juntar no fim);
  - por dados → **Decomposição Geométrica** (estruturas lineares como vetor e matriz; exemplo da **imagem**: contar cores dividindo a imagem em regiões; pode haver *overlap* entre regiões) e **Dados Recursivos** (árvores, grafos, que exigem recursão).
- **"Flow of data" (organização por fluxo de dados — Pipeline, Event-Based): NÃO vai cobrar.** "Só por tarefa e por dados."
- **Exemplo de pergunta conceitual que ela citou:** "Qual a diferença entre uma decomposição **em blocos** e uma decomposição **geométrica**?" → falar sobre os diferentes tipos de decomposição. (Blocos/cíclico são formas de **mapear/distribuir** os dados entre as unidades, na parte de balanceamento; a geométrica é o **padrão de estrutura** que divide os dados como regiões de uma geometria.)
- **Os 4 passos** (identificação do paralelismo → estrutura do algoritmo → estruturas de suporte → mecanismos de implementação), com ênfase nos **dois primeiros**, que foram aprofundados.
- **Grafo de dependência de tarefas** (§2.3).
- **Granularidade × número de threads** (§2.4).
- **Balanceamento estático × dinâmico** e ***work stealing*** (§2.5).
- **Leitura de código Java** com `ExecutorService` (§2.6).

Material: [06](06_padroes_de_paralelismo.md) e [07](07_threads_balanceamento_executor.md).

### 2.2 Exercício: definir decomposição de dados e de tarefas

Resposta da professora (a mesma do slide):
- **Dados:** dividir grandes conjuntos (vetores, matrizes, imagens) em **pedaços menores e independentes**; cada unidade de processamento recebe uma parte e faz a **mesma operação** — "lembra do exemplo do vetor".
- **Tarefas:** dividir o problema pelas **ações/funções/etapas**, independentemente dos dados; tarefas **distintas** rodam ao mesmo tempo.
- **Diferença:** "um, eu tô quebrando os **dados**; o outro, eu tô quebrando as **tarefas**".

### 2.3 Exercício: grafo de dependência (ProcessaVendas)

Resolvido em aula exatamente como na [Aula 12, §B2 ex. 2](12_joins_spark_revisao_prova.md#exercício-2--grafo-de-dependência-de-tarefas):
- Arestas: **T1 → T3**, **T1 → T4**, **T2 → T5**, **T3/T4/T5 → T6**. T2 não depende de T1 ("tô usando alguma variável da T1? Não").
- Paralelo: **T1 ‖ T2**; depois **T3 ‖ T4 ‖ T5**, "**desde que T1 e T2 tenham terminado**"; T6 sozinha.
- Padrão: decomposição **por tarefas** → **Task Parallelism**. "Não é divisão e conquista, porque não tem nenhuma divisão em subproblemas."

**Como responder na prova (discussão em aula):**
- Escreva os grupos paralelos **com a condição**: "T3, T4 e T5 podem executar em paralelo, **considerando que T1 e T2 já terminaram**". Ela insistiu: "é para deixar a resposta completa e bem clara".
- Alunos apontaram outras combinações válidas (ex.: **T1 ‖ T5, desde que T2 já tenha terminado**; **T3 e T4 junto com T2**, desde que T1 tenha terminado). Ela concordou que valem. Sobre listar **todas**, a fala ficou **ambígua** ("é para pegar um grupo só, não precisa pôr tudo"). **Recomendação:** dê os níveis principais com as condições e, se der tempo, cite as combinações cruzadas — ela disse que não tiraria muitos pontos, mas que quer a resposta completa.
- **Desenhe o grafo**: "é justamente por isso que a gente precisa do grafo, porque daí a gente consegue ver muito claramente" o que é independente.
- **Direção da seta:** um aluno perguntou se a seta não deveria apontar ao contrário. Ela ficou **em dúvida** em aula ("talvez só mude o direcionamento da seta"). O **slide** diz: a aresta vai **da tarefa precedente para a que consome sua saída** (T1 → T3). Use essa convenção e, se quiser, escreva uma legenda ("seta = produz → consome").

### 2.4 Exercício: granularidade × número de threads

Resposta (a do slide, explicada em aula):
- O trade-off é entre **balanceamento de carga** e **overhead de gerenciamento**.
- **Granularidade fina** (muitas tarefas pequenas, mais threads): menos núcleos ociosos, mas mais overhead de criação e sincronização → pode ficar mais lento.
- **Granularidade grossa** (poucas tarefas grandes, menos threads): overhead mínimo, mas risco de **desbalanceamento** e processadores ociosos.
- **Ideal:** tarefas pequenas o suficiente para ocupar todo o hardware e grandes o bastante para compensar o custo de gerenciá-las. "O maior objetivo é aproveitar o melhor possível as unidades de execução disponíveis."

**Exemplo discutido em aula (tarefas A, B e C, com A + B ≈ C em tempo, todas independentes):**

| Threads | Distribuição | Resultado |
|---|---|---|
| **2** | Thread 1: A depois B · Thread 2: C | **Equilibrado**: as duas terminam juntas. "Isso compensa." |
| **3** | Uma tarefa por thread | As threads de A e de B ficam **ociosas** esperando C terminar. Granularidade mais fina, sem ganho: "não é custo de overhead, é custo de ficar esperando" |
| **4**, quebrando C em C1 e C2 | Só ajuda se C1 e C2 forem **independentes**. Se C2 depender de C1, "vai ter que esperar de qualquer jeito, não adianta" | |

> **Mensagem dela:** esse tipo de análise "depende" — considere tamanhos das tarefas, dependências e número de unidades de execução, como nos exercícios de tempo da Aula 07.

### 2.5 Balanceamento de carga

- **Estático:** a divisão do trabalho é decidida **antes** da execução (compilação/inicialização), conhecendo o custo de cada tarefa. Quem define é, em geral, **o programador** (algumas linguagens têm recursos que ajudam).
- **Dinâmico:** decidido **durante** a execução; ex.: quando uma thread termina, já recebe outra tarefa.
- ***Work stealing*** (roubo de trabalho): pergunta de aluno; ela confirmou que é o algoritmo implementado no **Fork/Join** do Java e que é **dinâmico** ("vai terminando e vai pegando").

### 2.6 Exercício: programa Java (`RastreioPool`)

Pergunta: **quantas tarefas e quantas threads, e qual a estratégia de decomposição?** (Código na [Aula 12, §B2 ex. 4](12_joins_spark_revisao_prova.md#exercício-4--programa-java-rastreiopool).)

| Item | Resposta |
|---|---|
| Threads | **2** do pool (`newFixedThreadPool(2)`) — "só por essa linha a gente já consegue ver" |
| Tarefas | **4** (o `for` anda de 3 em 3 sobre 10 elementos: blocos 0–2, 3–5, 6–8, 9) |
| Decomposição | **De dados** (partição do vetor em blocos) |

> **Atenção a duas confusões que aconteceram em aula:**
> 1. A professora contou primeiro "quatro" e depois disse "**três tarefas**". O correto é **4**: os índices de início são 0, 3, 6 e 9.
> 2. Uma aluna lembrou que, contando a **thread `main`**, seriam **3** threads. A professora concordou e prometeu deixar a pergunta **mais exata na prova** (ex.: "quantas threads o executor cria" ou "considerando a execução do programa"). **Na prova, se a pergunta não for explícita, responda as duas coisas:** "2 threads no pool, além da thread principal (`main`), total de 3".

---

## 3. Big Data, Hadoop e HDFS

### 3.1 O que ela vai cobrar

- **Características do Big Data: os 5 Vs** (Volume, Velocidade, Variedade, Veracidade, Valor) **e a definição** de cada um.
- **Arquitetura do Hadoop:** "explicar o que é o **NameNode**, o que é o **DataNode** e a arquitetura geral do Hadoop — é uma coisa que eu posso cobrar".
  - **NameNode** (mestre): **gerencia** os arquivos — abre, fecha, renomeia etc. **Não armazena dados** ("o NameNode não armazena, ele gerencia; é sempre o DataNode que armazena").
  - **DataNode** (workers): **armazena** blocos dos arquivos e cuida da leitura/escrita deles; é **nele que o *map* executa** (levar o código até os dados).
- **Exercício de HDFS** (blocos + replicação) e **exercício de MapReduce** (entradas e saídas de cada fase).
- **Vantagens e desvantagens do MapReduce.**

Material: [08, Bloco B](08_forkjoin_bigdata_hadoop.md).

### 3.2 Exercício de HDFS (blocos de 4 MB, 3 workers de 8 MB, replicação 2)

Resolvido em aula como na [Aula 12, §B2 ex. 5](12_joins_spark_revisao_prova.md#exercício-5--hdfs-blocos-e-replicação): **B1 = linhas 02–05 (4 MB)**, **B2 = 06–09 (4 MB)**, **B3 = 10 (1 MB)**; réplicas: W1 = B1 + B3, W2 = B2 + B1, W3 = B3 + B2.

**Regras que ela destacou:**
- **Particione na ordem das linhas**, sem reorganizar: o HDFS lê o arquivo **por offset de bytes**, preenchendo 4 MB, depois mais 4 MB, e o resto. ("Não pode ser [linhas fora de ordem]… é por ordem.")
- **Pegadinha:** a lista começa na **Linha 02** (não há Linha 01). Na aula, a turma errou a contagem e ela disse: "**era pegadinha**". **Conte as linhas com cuidado.**
- **Fator de replicação 2 = a cópia original + 1 réplica**, em **outro** worker. (Um aluno lembrou que o padrão do Hadoop é 3; ela confirmou que o fator conta o **total** de cópias.)
- **Não estoure a capacidade** do worker: W2 chega a **8 MB (cheio)**, então nada mais cabe nele. "Se estourar o espaço total, eu tenho que jogar para outro lado."
- **Verifique a tolerância a falhas:** se **qualquer um** dos workers morrer, todo bloco ainda precisa ter uma cópia em outro. Na solução, isso vale.
- Na prática o Hadoop faz isso automaticamente; o exercício é para entender o mecanismo.

### 3.3 Exercício de MapReduce (contagem de códigos de status)

Resolvido como na [Aula 12, §B2 ex. 6](12_joins_spark_revisao_prova.md#exercício-6--mapreduce-contagem-de-códigos-de-status). **Formato de resposta que ela pediu:**

1. **Saída do Map** (em cada worker);
2. **Entrada do Shuffle** (= saída do Map);
3. **Entrada de cada Reducer** (chaves agrupadas: `("200 OK",[1,1,1])`, `("404 Not Found",[1,1])` / `("500 Error",[1])`);
4. **Saída de cada Reducer** (resultado final: 200 → 3, 404 → 2, 500 → 1).

**Pontos que ela enfatizou:**
- "A gente vai **sempre** trabalhar com a ideia de **chave-valor**."
- **O Map não conta:** só emite `(chave, 1)` para cada linha. "Eu não conto nesta parte, eu vou contar só lá no reduce."
- O **shuffle/sort agrupa as mesmas chaves** e as envia para o **mesmo reducer**; há **movimentação de dados** pela rede, e os resultados intermediários vão para **disco**.
- Qual grupo vai para qual reducer pode variar; o importante é que cada chave inteira vá para um só reducer.
- O `print` do resultado no fim **não faz parte** do Reduce.
- "O melhor exemplo para ver cada passo é o **word count** que está no slide."
- Uma pergunta ficou **sem resposta** em aula: o que acontece se o disco dos workers estiver cheio na hora do shuffle. Ela disse que vai verificar e colocar nas notas dos slides.

### 3.4 Vantagens e desvantagens do MapReduce

- **Vantagem:** fácil de implementar — **só duas funções** (map e reduce); escala para grandes volumes.
- **Desvantagens:** (1) **nem todo algoritmo** se traduz bem em map + reduce (limitado pelo modelo); (2) resultados intermediários em **disco** (o problema do Hadoop); (3) o **shuffle/sort** afeta diretamente o desempenho ("shuffle mata tudo", como resumiu um aluno).

---

## 4. NoSQL, ACID × BASE e CAP

### 4.1 O que ela vai cobrar

- **Tipos de bancos NoSQL:** **chave-valor**, **documentos**, **grafos** e **colunares** (e a ideia de cada um).
- **ACID × BASE:** o que é cada um e **a desvantagem do BASE**.
- **Teorema CAP** — "Teorema CAP **também**, tá? Lembrem o que é BASE, o que é ACID e o que é o teorema CAP."
- **Não** vai pedir o significado de cada **letra** da sigla, mas a **ideia**.

Material: [09, Bloco A](09_nosql_cap_spark_rdd.md).

### 4.2 Explicação dela

- **ACID** (bancos relacionais, como em Transações): a transação é **atômica** (ou comita, ou dá *rollback*) e leva o banco de um **estado consistente a outro** (restrições de integridade continuam valendo). **Garante que os dados estão corretos.**
- **BASE** (comum em NoSQL, **mas não em todos**): o sistema **sempre dá uma resposta** (*basically available*); o estado pode mudar com o tempo por causa da replicação (*soft state*); os dados ficam corretos **em algum momento** (*eventually consistent*). **Pode acontecer uma consulta no meio da atualização que pega o resultado errado.**
- **Diferença:** ACID garante **dados corretos**; BASE garante **disponibilidade**.
- **Desvantagem do BASE:** **não garante** que o resultado da consulta esteja correto, o que pode causar problemas.
- **Neo4j** (grafos) é **ACID**, apesar de NoSQL. Um aluno sugeriu que era "porque é implementado em SQL"; ela **corrigiu**: é ACID porque **garante as propriedades da transação** (commit atômico, consistência).
- **"*Eventually*"** não é "eventualmente" no sentido de "talvez": é "**em algum momento**" (no fim, vai acontecer). Ela acrescentou que, na prática, pode haver casos em que a consistência demora ou não acontece, e isso é um problema para consultas.

---

## 5. Lista completa de tópicos (dita no começo da aula)

1. **Programação paralela:** ideia geral, padrões, decomposição de dados e de tarefas, balanceamento de carga, programação paralela em Java.
2. **Arquivos distribuídos e Big Data:** conceitos de Big Data, Hadoop, HDFS, introdução à programação funcional com Scala, Spark (execução e programação), SparkSQL.
3. **Processamento de consulta:** passo a passo do processamento, introdução à álgebra relacional, otimização baseada em heurísticas, ideia geral da otimização baseada em custo e estimativa de cardinalidade, principais algoritmos de junção.

> Ela comentou que **foi muito conteúdo** ("conteúdo para caramba").

---

## 6. Resumo: o que estudar, por prioridade

| Prioridade | Tópico | Formato provável | Onde |
|---|---|---|---|
| **Alta** | Grafo de dependência + tarefas paralelas + padrão | Exercício (com condições na resposta) | [06](06_padroes_de_paralelismo.md), [12 §B2 ex. 2](12_joins_spark_revisao_prova.md#exercício-2--grafo-de-dependência-de-tarefas) |
| **Alta** | Decomposição de dados × tarefas; Task Parallelism, D&C, Geométrica, Dados Recursivos | Conceitual (definição, quando usar, exemplo, diferenças) | [06](06_padroes_de_paralelismo.md) |
| **Alta** | Granularidade × threads; balanceamento estático × dinâmico; work stealing | Dissertativa | [07](07_threads_balanceamento_executor.md) |
| **Alta** | Ler código Java com `ExecutorService` (tarefas, threads, decomposição) | Interpretação | [07](07_threads_balanceamento_executor.md), [12 §B2 ex. 4](12_joins_spark_revisao_prova.md#exercício-4--programa-java-rastreiopool) |
| **Alta** | HDFS: blocos na ordem, replicação, capacidade | Exercício | [08](08_forkjoin_bigdata_hadoop.md), [12 §B2 ex. 5](12_joins_spark_revisao_prova.md#exercício-5--hdfs-blocos-e-replicação) |
| **Alta** | MapReduce: entradas/saídas de map, shuffle e reduce | Exercício | [08](08_forkjoin_bigdata_hadoop.md), [12 §B2 ex. 6](12_joins_spark_revisao_prova.md#exercício-6--mapreduce-contagem-de-códigos-de-status) |
| Média | 5 Vs; NameNode × DataNode; vantagens/desvantagens do MapReduce | Conceitual | [08](08_forkjoin_bigdata_hadoop.md) |
| Média | Tipos de NoSQL; ACID × BASE; CAP | Conceitual | [09](09_nosql_cap_spark_rdd.md) |
| **Pendente (aula extra)** | Spark (DAG, quando executa), SparkSQL → álgebra, heurísticas, escolha do algoritmo de join | Exercícios dos slides | [09](09_nosql_cap_spark_rdd.md), [10](10_spark_pratica_sparksql.md), [11](11_processamento_otimizacao_consultas.md), [12 §B2 ex. 7–10](12_joins_spark_revisao_prova.md#exercício-7--spark-dag-e-quando-executa) |
| **Não cai** | Organização por fluxo de dados (Pipeline, Event-Based); escrever código; significado letra a letra das siglas | — | — |

---

## 7. O que ficou pendente

- A professora parou às 10h20 sem terminar. **Faltaram os exercícios** da parte de **Spark**, **SparkSQL/álgebra relacional**, **otimização de consultas** e **algoritmos de join** — ela disse que eram "bem importantes".
- Combinado: tentar pegar a aula de outro professor; **no pior caso, aula extra na quinta-feira, no mesmo horário**.
- Enquanto isso, esses exercícios já estão **resolvidos** no material: [Aula 12, §B2, exercícios 7 a 10](12_joins_spark_revisao_prova.md#exercício-7--spark-dag-e-quando-executa).
- Dica dela para quem quiser aprofundar programação paralela: o curso **CS149 de Stanford** (de onde vieram algumas figuras dos slides).
