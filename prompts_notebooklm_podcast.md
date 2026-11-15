# Prompts para o NotebookLM (Aulas 06 a 12)

**Como usar:** crie um caderno por aula, adicione como fontes o `.md` (de `estudo/`) e o `.pdf` da aula (de `fontes/aulas/`) indicados, e, se quiser, os livros como apoio (o prompt os trata como complementares), abra **Personalizar** no resumo em áudio e cole o prompt da aula.

---

## Aula 06 — Identificando o paralelismo e escolhendo a estrutura do algoritmo
**Fontes:** `estudo/06_padroes_de_paralelismo.md` + `fontes/aulas/Aula6_1209.pdf`

```text
Contexto: estou me preparando para a Prova 2 de Computação Escalável da professora Larissa. Foque nas fontes 06_padroes_de_paralelismo.md (material de estudo) e Aula6_1209.pdf (slides da aula); livros, se houver no caderno, são apenas complementares. Use somente o conteúdo dessas fontes e cubra todos os tópicos, na ordem: SIMD × MIMD e os 4 passos para paralelizar; decomposição por tarefas × dados (exemplo de 1 milhão de registros) e critérios de uma boa decomposição; dependências e grafo de tarefas; os 6 padrões (Task Parallelism, Divide and Conquer, Geometric Decomposition, Recursive Data, Pipeline, Event-Based) e a árvore de decisão; discussões da aula (KNN, ensemble, SQL JOIN). Sinalize o que costuma cair em prova. No final, faça uma revisão rápida de todos os tópicos explicados.
```

---

## Aula 07 — Granularidade, balanceamento, localidade e ExecutorService
**Fontes:** `estudo/07_threads_balanceamento_executor.md` + `fontes/aulas/Aula7_2509.pdf`

```text
Contexto: estou me preparando para a Prova 2 de Computação Escalável da professora Larissa, que cobra leitura de código Java (o que faz, onde usar, se está correto). Foque nas fontes 07_threads_balanceamento_executor.md (material de estudo) e Aula7_2509.pdf (slides da aula); livros, se houver no caderno, são apenas complementares. Use somente o conteúdo dessas fontes e cubra todos os tópicos, na ordem: decomposição → threads; task × thread × chunk; granularidade (CPU-bound × I/O-bound); balanceamento estático × dinâmico; mapeamento em blocos, cíclico e aleatório; Master/Slave e load balancer (Round Robin × Least Connection); localidade de dados; Thread/Runnable; ExecutorService e tipos de pool; Callable e Future; introdução ao ForkJoin; exercício da soma com 3 Callables; erros típicos de código. Descreva o código em palavras, sem ler símbolos. Sinalize o que costuma cair em prova. No final, faça uma revisão rápida de todos os tópicos explicados.
```

---

## Aula 08 — Fork/Join, Big Data, Hadoop, HDFS, MapReduce e Spark
**Fontes:** `estudo/08_forkjoin_bigdata_hadoop.md` + `fontes/aulas/aula8-0910.pdf`

```text
Contexto: estou me preparando para a Prova 2 de Computação Escalável da professora Larissa, que cobra leitura de código Java e conceitos de Big Data. Foque nas fontes 08_forkjoin_bigdata_hadoop.md (material de estudo) e aula8-0910.pdf (slides da aula); livros, se houver no caderno, são apenas complementares. Use somente o conteúdo dessas fontes e cubra todos os tópicos, na ordem. Bloco A (Fork/Join): fork/join e work stealing; RecursiveTask × RecursiveAction; BuscaMaiorValorTask passo a passo; limiar; ExecutorService × ForkJoin; exercícios; erros típicos. Bloco B (Big Data): 5 Vs; Hadoop; HDFS (NameNode/DataNode, blocos de 128 MB, replicação 3); MapReduce (map, shuffle, reduce, combiner, word count); Spark e Hadoop × Spark. Descreva o código em palavras, sem ler símbolos. Sinalize o que costuma cair em prova. No final, faça uma revisão rápida de todos os tópicos explicados.
```

---

## Aula 09 — NoSQL, CAP/BASE, programação funcional e o Spark por dentro
**Fontes:** `estudo/09_nosql_cap_spark_rdd.md` + `fontes/aulas/aula9_2010.pdf`

```text
Contexto: estou me preparando para a Prova 2 de Computação Escalável da professora Larissa. Foque nas fontes 09_nosql_cap_spark_rdd.md (material de estudo) e aula9_2010.pdf (slides da aula); livros, se houver no caderno, são apenas complementares. Use somente o conteúdo dessas fontes e cubra todos os tópicos, na ordem. A: limites do MapReduce (WORM), NoSQL (chave-valor, documentos, grafos, colunares), teorema CAP (CP × AP, exemplo do banco), BASE × ACID. B: programação funcional (imutabilidade, lambda, função pura, ordem superior) e por que combina com processamento distribuído. C: Spark por dentro (Driver, Executors, Cluster Manager), RDD, particionamento, DAG e lineage, lazy evaluation e cache, Pair RDDs, shuffle e transformações estreitas × largas. Descreva o código em palavras. Sinalize o que costuma cair em prova. No final, faça uma revisão rápida de todos os tópicos explicados.
```

---

## Aula 09b — Programação em Scala
**Fontes:** `estudo/09b_programacao_scala.md` + `fontes/aulas/aula_programacao_scala_2110.pdf`

```text
Contexto: estou me preparando para a Prova 2 de Computação Escalável da professora Larissa, que cobra leitura de código Scala (o que imprime, se está correto). Conheço Java. Foque nas fontes 09b_programacao_scala.md (material de estudo) e aula_programacao_scala_2110.pdf (slides da aula); livros, se houver no caderno, são apenas complementares. Use somente o conteúdo dessas fontes e cubra todos os tópicos, na ordem: visão geral do Scala; estrutura de um programa; val × var; if e for; def, lambdas e o underscore; class, case class e trait; Option; coleções imutáveis × mutáveis; map, flatMap, filter, reduce, groupBy; WordCount com coleções; os 4 exercícios resolvidos; erros típicos. Não leia código símbolo a símbolo: descreva em palavras e compare com Java. Sinalize o que costuma cair em prova. No final, faça uma revisão rápida de todos os tópicos explicados.
```

---

## Aula 10 — Spark na prática: RDDs, DataFrames e SparkSQL
**Fontes:** `estudo/10_spark_pratica_sparksql.md` + `fontes/aulas/aula10_3010_spark_introducao_a_processamento.pdf`

```text
Contexto: estou me preparando para a Prova 2 de Computação Escalável da professora Larissa, que cobra leitura de código Spark em Scala (quando executa, onde há shuffle, se está correto). Foque nas fontes 10_spark_pratica_sparksql.md (material de estudo) e aula10_3010_spark_introducao_a_processamento.pdf (slides da aula); livros, se houver no caderno, são apenas complementares. Use somente o conteúdo dessas fontes e cubra todos os tópicos, na ordem: modos local/standalone/cluster; SparkContext × SparkSession; RDDs (parallelize, partições, repartition × coalesce, reduceByKey, toDebugString); HashPartitioner, particionador próprio e data skew; RDD × DataFrame × Dataset; Catalyst; DataFrames e SQL; como ler o explain; tipos de join; escrita com partitionBy e 200 partições de shuffle; Docker e spark-submit; os 4 exercícios refeitos em Spark; erros típicos. Descreva o código em palavras. Sinalize o que costuma cair em prova. No final, faça uma revisão rápida de todos os tópicos explicados.
```

---

## Aula 11 — Processamento e otimização de consultas
**Fontes:** `estudo/11_processamento_otimizacao_consultas.md` + `fontes/aulas/aula_otimizacao_de_consulta_11_0611.pdf` (opcional: adicionar `estudo/11a_introducao_algebra_relacional.md` para a notação)

```text
Contexto: estou me preparando para a Prova 2 de Computação Escalável da professora Larissa, que cobra desenhar e otimizar árvores de consulta e fazer contas de seletividade e cardinalidade. Foque nas fontes 11_processamento_otimizacao_consultas.md (material de estudo) e aula_otimizacao_de_consulta_11_0611.pdf (slides da aula); livros, se houver no caderno, são apenas complementares. Use somente o conteúdo dessas fontes e cubra todos os tópicos, na ordem: etapas do processamento; varredura, análise e validação; árvore de consulta e pós-ordem; equivalências algébricas (outer join não comuta); heurísticas (selection e projection push-down, trocar produto cartesiano por junção); algoritmo de 6 passos e exercício Aquarius passo a passo; otimização por custo; seletividade; nested loops, sort-merge e hash join; fator de seleção de junção; cardinalidade, data skew e histogramas; ponte com o Spark. Narre as árvores de baixo para cima e faça as contas em voz alta. Sinalize o que costuma cair em prova. No final, faça uma revisão rápida de todos os tópicos explicados, incluindo as fórmulas.
```

---

## Aula 12 — Algoritmos de join no Spark e exercícios para a prova
**Fontes:** `estudo/12_joins_spark_revisao_prova.md` + `fontes/aulas/aula12_algoritmos_join_spark.pdf`

```text
Contexto: estou me preparando para a Prova 2 de Computação Escalável da professora Larissa; esta é a última aula antes da prova. Foque nas fontes 12_joins_spark_revisao_prova.md (material de estudo) e aula12_algoritmos_join_spark.pdf (slides da aula); livros, se houver no caderno, são apenas complementares. Use somente o conteúdo dessas fontes e cubra todos os tópicos, na ordem: por que o join é caro no Spark (shuffle); Broadcast Join (Broadcast Hash Join e Broadcast Nested Loop Join, limite de 10 MB); Shuffled Hash Join; Shuffled Sort-Merge Join e por que é a escolha comum; como ler BroadcastHashJoin e SortMergeJoin no explain; o que o Catalyst fez no notebook (constant folding, filtro empurrado para antes do join, PushedFilters); UDFs e AQP; a lista de tópicos da prova; e os 10 exercícios de revisão resolvidos, um por um. Descreva código e planos em palavras. Sinalize o que costuma cair em prova. No final, faça uma revisão rápida de todos os tópicos explicados.
```

---

## Aula 11a — Introdução à Álgebra Relacional (na lista de tópicos da P2)
**Fontes:** `estudo/11a_introducao_algebra_relacional.md` + `fontes/aulas/Introducao_Algebra_Relacional.pdf` · Está na lista de tópicos da P2 (introdução a álgebra relacional); sugestão: ouvir antes da Aula 11.

```text
Contexto: estou me preparando para a Prova 2 de Computação Escalável da professora Larissa; este conteúdo está na lista de tópicos da prova e é a base de notação para a aula de otimização de consultas e para exercícios de SparkSQL → álgebra relacional. Foque nas fontes 11a_introducao_algebra_relacional.md (material de estudo) e Introducao_Algebra_Relacional.pdf (slides da aula); livros, se houver no caderno, são apenas complementares. Use somente o conteúdo dessas fontes e cubra todos os tópicos, na ordem: SQL × álgebra relacional, closure e conjuntos; seleção, projeção, renomeação; produto cartesiano e junção; união, interseção e subtração; divisão; agrupamento; árvore de operações; mapa álgebra ↔ SQL; os 4 exercícios. Diga os símbolos pelo nome. No final, faça uma revisão rápida de todos os operadores.
```

---

**Se algum tópico for pulado**, acrescente ao final do prompt: *"Antes de começar, liste a ordem dos tópicos e siga-a sem pular nenhum."*
