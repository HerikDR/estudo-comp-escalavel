# Aula 09 — NoSQL, CAP/BASE e o Spark por dentro (versão P2)

> Aula de 20/10 · Profa. Larissa C. Shimomura
> **Filtro:** [12c](12c_orientacoes_prova2_revisao2.md) e [12b](12b_orientacoes_prova2.md).
> **Cai:** tipos de NoSQL; ACID × BASE (e a desvantagem do BASE); CAP e "CP/AP" ("lembra das nomenclaturas"); **DAG** de um RDD e para que ele serve; *lazy evaluation*. **Não** pede o significado letra a letra das siglas.
> **Cortado desta versão:** código Scala (lambdas, `criarMultiplicador`, exemplos executados), treino de leitura de código, `toDebugString`, detalhes de `persist`. Programação funcional ficou só como conceito (estava na lista de tópicos, mas nenhuma revisão a citou). A versão completa está no histórico do git.

---

## 1. Por que ir além do MapReduce e do relacional

- **MapReduce:** só duas fases; nem todo algoritmo cabe; intermediários em **disco**; shuffle caro. O HDFS é **WORM** (*write once, read many*): ruim para escritas frequentes.
- **Relacional:** escala mal **na horizontal** e tem **esquema rígido** (ruim para dados semi/não estruturados).
- Daí: **NoSQL** para armazenar e **Spark** para processar.

---

## 2. Bancos NoSQL ("*Not only SQL*")

| Tipo | Ideia | Exemplo | Uso típico |
|---|---|---|---|
| **Chave-valor** | Pares (chave, valor), como um **dicionário**; a chave é um ID único que acelera a consulta | Redis | Sessões de usuário |
| **Documentos** | Registros **semi-estruturados e flexíveis** em JSON/BSON; cada documento é **autocontido** | MongoDB | Catálogos, perfis |
| **Grafos** | **Nós** e **arestas**; consultas por caminhos e relacionamentos | Neo4j, Neptune | Redes sociais, recomendação |
| **Colunares** | Dados armazenados **por coluna**, não por linha | Cassandra, HBase | Consultas analíticas |

O **Neo4j é ACID** apesar de ser NoSQL, porque **garante as propriedades da transação** (e não "porque usa SQL").

---

## 3. ACID × BASE

| | **ACID** | **BASE** |
|---|---|---|
| Ideia | A transação é **commitada**: ou atualiza todo mundo, ou ninguém. Vai de um estado consistente a outro | "Estou **sempre disponível** para responder; mas posso **não atualizar na hora**: respondo e continuo atualizando **em background**" |
| Garante | Dados **corretos**, mesmo que precise reportar falha | **Disponibilidade** |
| Lado do CAP | **CP** | **AP** |
| Onde | Bancos relacionais, Neo4j | Muitos NoSQL (**não todos**) |

**BASE** = *Basically Available* (responde quase sempre), *Soft state* (o estado muda sozinho por causa da replicação em segundo plano), *Eventually consistent* (**em algum momento** todos os nós convergem, se não houver novas escritas).

**Desvantagem do BASE (Ex. 1 da lista P2):** tolera **divergências temporárias** entre os nós. Uma consulta feita **antes de a sincronização terminar** pode devolver dados **desatualizados ou inconsistentes**. O ACID garante dado correto; o BASE, não. Gabarito da lista: o BASE abre mão da consistência imediata em favor de **disponibilidade e escalabilidade horizontal**; responde rápido e sincroniza as cópias em segundo plano.

> "Teorema BASE" é como a professora chama; tecnicamente é um **modelo**, e o teorema é o CAP.

---

## 4. Teorema CAP

Em sistemas **distribuídos** é impossível garantir **ao mesmo tempo**:

| | Propriedade | Definição |
|---|---|---|
| **C** | Consistência | Todos os nós retornam **a mesma informação atualizada no mesmo instante** |
| **A** | Disponibilidade | **Toda requisição recebe resposta**, mesmo que não seja a versão mais recente |
| **P** | **Tolerância a particionamento** (não é "persistência") | O sistema **continua funcionando** com falhas de comunicação entre os nós |

**Falhas de rede são inevitáveis ⇒ P é obrigatório ⇒ escolhe-se entre C e A**: sistemas **CP** ou **AP**.

**Exemplo do banco** (A transfere para B e um nó falha durante a transação):
- **CP:** a transação **não é commitada** e o cliente **fica sem resposta** sobre o saldo.
- **AP:** o cliente **recebe resposta, mas com o saldo incorreto**.

**Nomenclatura:** **BASE ≈ AP**; **ACID ≈ CP**. O CAP vale para **sistemas distribuídos em geral**; o BASE vem do lado dos **bancos de dados**, como contraponto ao ACID ("bastante parecidos").

---

## 5. Programação funcional (só a ideia)

| Conceito | Por que serve ao processamento distribuído |
|---|---|
| **Imutabilidade** (operações criam dados novos) | Cada tarefa trabalha na sua partição sem disputar o mesmo dado: sem condição de corrida |
| **Funções puras** (mesma entrada → mesma saída, sem efeito colateral) | É seguro executar em qualquer nó e **reexecutar** depois de uma falha |
| **Funções de ordem superior** (`map`, `filter`, `reduce` recebem funções) | O programador **descreve** a transformação e o framework decide onde e em paralelo aplicá-la |

O Spark foi escrito em **Scala** (funcional + OO, roda na JVM).

---

## 6. Spark por dentro

### 6.1 Arquitetura

- **Driver:** roda o `main`, usa o `SparkContext`, **monta o DAG** e distribui/agenda as tarefas.
- **Executors:** executam as tarefas em paralelo e guardam dados em memória/disco.
- **Cluster Manager:** aloca CPU e memória do cluster.

### 6.2 RDD

- *Resilient Distributed Dataset*: estrutura **imutável e distribuída**, dividida em **partições**. Cada operação gera um **novo** RDD.
- As partições seguem a camada de armazenamento (com HDFS, ~1 bloco = 1 partição); sem ela, o particionamento é **lógico**, em memória. O Driver não lê os dados: só diz a cada executor "leia do byte X ao Y".

### 6.3 Transformações, ações, DAG e *lineage*

| | Transformação | Ação |
|---|---|---|
| Exemplos | `parallelize`/`textFile`, `filter`, `map`, `reduceByKey`, `join` | `collect`, `count`, `take`, `reduce`, `show`, escrita |
| Executa? | **Não**: só é anotada no DAG (***lazy evaluation***) | **Sim**: é o **gatilho** que faz o Driver mandar as tasks |

- **DAG:** grafo que **acumula todas as transformações** do código (inclusive a criação do RDD), no mesmo sentido do grafo de dependência. É diferente da **árvore de consulta**, que só tem operadores relacionais.
- ***Lineage*:** o histórico de transformações que leva do dado original ao RDD.

**Para que serve o DAG (Ex. 3 da lista P2):**
1. ***Lazy evaluation*** — registra a sequência de transformações **antes** de executar, o que permite **otimizar o plano** e encadear as operações em memória quando a ação é chamada, sem gravar intermediários em disco.
2. **Tolerância a falhas** — se uma partição se perde, o Spark **recomputa só as partições perdidas** a partir das etapas anteriores do DAG. (No Hadoop, a tolerância vem da **replicação de blocos**.)

**Lista P2, Ex. 3:**

```scala
val linhas = sc.textFile("logs.txt")
val erros = linhas
  .filter(linha => linha.contains("ERROR"))
  .map(linha => (linha.split(";")(1), 1))
  .reduceByKey((a, b) => a + b)
val resultado = erros.collect()
```

DAG esperado: `textFile → filter → map → reduceByKey → collect()` (desenhe **só as operações**, sem os dados). A execução começa no **`collect()`**. O exercício resolvido em aula (`rdd_prova`) está em [12, Ex. 7](12_joins_spark_revisao_prova.md#exercício-7--spark-dag-e-quando-executa).

> **Custo do lazy:** se dois ramos derivam do mesmo RDD, a parte comum é recalculada a cada ação. Solução: `cache()`/`persist()`.

### 6.4 Shuffle: transformações estreitas × largas

- **Estreita** (`map`, `filter`, `flatMap`, `mapValues`): cada partição de saída depende de **uma** partição de entrada; roda local, sem rede.
- **Larga** (`reduceByKey`, `groupByKey`, `join`, `distinct`, `sortByKey`): uma partição de saída depende de **várias** de entrada → **shuffle** (redistribui os dados pela rede, o que é caro).
- **Pair RDDs** (RDDs chave-valor) têm métodos próprios: `reduceByKey`, `groupByKey`, `join`, `sortByKey`.

### 6.5 Balanço do Spark

| Vantagens | Desvantagens |
|---|---|
| Flexível (lote, SQL, ML, streaming) | Gerenciar a **memória** |
| Resolve em parte o desempenho do Hadoop (memória em vez de disco) | Curva de aprendizado, muitos parâmetros |
| | O **shuffle continua caro** |

---

## 7. Checklist

- [ ] Cito os 4 tipos de NoSQL com a ideia de cada um
- [ ] Explico ACID × BASE e a desvantagem do BASE
- [ ] Explico C, A e P, por que P é obrigatório e o exemplo do banco em CP e AP
- [ ] Sei que BASE ≈ AP e ACID ≈ CP
- [ ] Desenho o DAG de um código Spark e digo em que linha a execução começa
- [ ] Explico para que serve o DAG (lazy evaluation + tolerância a falhas)
- [ ] Diferencio transformação estreita × larga (shuffle)
