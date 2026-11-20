# Aula 08 — Big Data, Hadoop, HDFS, MapReduce e Spark (versão P2)

> Aula de 09/10 · Profa. Larissa C. Shimomura
> **Filtro:** [12c](12c_orientacoes_prova2_revisao2.md) e [12b](12b_orientacoes_prova2.md). A 2ª revisão não mencionou Big Data/HDFS/MapReduce, então vale o que foi dito na 1ª revisão: **cai** (5 Vs, NameNode × DataNode, exercício de HDFS, exercício de MapReduce, vantagens/desvantagens do MapReduce). A lista P2 traz HDFS (Ex. 5) e Hadoop × Spark (Ex. 4).
> **Cortado desta versão** (não cai): todo o **Bloco A — Fork/Join em Java** (código `RecursiveTask`, `fork`/`compute`/`join`, exercícios e treino); o WordCount na API Java do Hadoop; módulos do Spark e detalhes de YARN/racks. Do Fork/Join fica só a ideia: é **Divisão e Conquista** com balanceamento **dinâmico por *work stealing*** ([07](07_threads_balanceamento_executor.md)). A versão completa está no histórico do git.

---

## 1. Big Data: os 5 Vs

Quando os dados não cabem em um nó, usa-se um **sistema distribuído**, em que **cada nó guarda parte dos dados**.

| V | Significado |
|---|---|
| **Volume** | Quantidade **massiva** de dados |
| **Velocidade** | Rapidez com que **novos dados são produzidos** |
| **Variedade** | De **estruturados** (tabelas) a **não estruturados** (texto, áudio, vídeo, logs) |
| **Veracidade** | **Confiabilidade, precisão e qualidade** dos dados |
| **Valor** | Transformar os dados em **insights** e vantagem competitiva |

---

## 2. Hadoop

| Característica | Significado |
|---|---|
| ***Commodity hardware*** | Cluster de máquinas comuns, não supercomputadores |
| **Tolerância a falhas** | Se uma máquina cai, usa **outra cópia** do dado e continua em outra máquina |
| **Levar o código aos dados** | O processamento roda **no nó onde o dado está**, em vez de mover terabytes pela rede |
| ***Shared-nothing*** | Cada máquina tem CPU, memória e disco próprios |

---

## 3. HDFS

### 3.1 NameNode × DataNode

| | **NameNode** (mestre) | **DataNode** (workers) |
|---|---|---|
| Função | **Gerencia** os arquivos: abre, fecha, renomeia; guarda os **metadados** e o mapeamento arquivo → blocos → DataNodes | **Armazena** os blocos e atende leituras/escritas; é **nele que o *map* executa** |
| Guarda os dados? | **Não** ("o NameNode não armazena, ele gerencia") | **Sim** |

DataNodes mandam *heartbeats* ao NameNode; se um para, o NameNode o considera morto e **re-replica** os blocos dele.

### 3.2 Blocos e replicação (regras do exercício)

- O arquivo é dividido em **blocos de tamanho fixo** (padrão 128 MB) **por *byte offset***, **na ordem das linhas**, sem reorganizar. O **último bloco ocupa só o necessário** (300 MB = 128 + 128 + **44**).
- **Fator de replicação = total de cópias** (fator 2 = original + 1 réplica; padrão 3).
- **Duas réplicas do mesmo bloco nunca ficam no mesmo nó.**
- **Não estoure a capacidade** de nenhum worker.
- **Confira a tolerância a falhas:** se qualquer worker cair, todo bloco ainda precisa ter cópia em outro.
- Cuidado com **pegadinhas de contagem de linhas** (na 1ª revisão a lista começava na linha 02).

**Lista P2, Ex. 5** — 5 workers (W1–W5) de 20 MB, bloco máximo de 10 MB, 1 linha = 1 MB. Clientes: 15 linhas; Pedidos: 10 linhas. Replicação 3.

(a) Blocos: **BC1** = Clientes linhas 1–10 (10 MB); **BC2** = Clientes 11–15 (5 MB, bloco residual); **BP1** = Pedidos 1–10 (10 MB). Cada relação é dividida separadamente.

(b) Uma distribuição válida (3 cópias de cada, em nós distintos, sem passar de 20 MB):

| W1 (20/20) | W2 (15/20) | W3 (15/20) | W4 (15/20) | W5 (10/20) |
|---|---|---|---|---|
| BC1 + BP1 | BC1 + BC2 | BC1 + BC2 | BP1 + BC2 | BP1 |

Modelo resolvido em aula (blocos de 4 MB, replicação 2): [12, Ex. 5](12_joins_spark_revisao_prova.md#exercício-5--hdfs-blocos-e-replicação).

---

## 4. MapReduce

**Ideia:** processar cada bloco **no próprio nó** (localidade) e dividir para conquistar. O programador escreve só **`map`** e **`reduce`**; o framework faz o resto.

| Fase | O que faz |
|---|---|
| **Map** | Em cada bloco, emite pares **(chave, valor)**. **O map não conta**: emite `(chave, 1)` por ocorrência |
| **Shuffle / sort** | **Agrupa as mesmas chaves** e as envia ao **mesmo reducer**; há **movimentação de dados pela rede** e os intermediários vão para **disco** |
| **Reduce** | Recebe `(chave, [valores])` e produz o resultado (ex.: soma). É **opcional** |

**Formato de resposta pedido na 1ª revisão:** (1) saída do Map em cada worker → (2) entrada do shuffle (= saída do map) → (3) entrada de cada Reducer (chaves agrupadas, ex.: `("200 OK",[1,1,1])`) → (4) saída de cada Reducer. "Sempre chave-valor." O `print` final não faz parte do reduce. Modelo: [12, Ex. 6](12_joins_spark_revisao_prova.md#exercício-6--mapreduce-contagem-de-códigos-de-status).

**Word count (slide):** linhas → map `(palavra, 1)` → shuffle agrupa `Apple: [1,1,1,1]`, `Plum: [1,1,1]`… → reduce soma → `(Apple, 4)`, `(Plum, 3)`…

| Vantagens | Desvantagens |
|---|---|
| Fácil: **só duas funções** | **Nem todo algoritmo** se expressa em map + reduce |
| Escala para grandes volumes; tolera falhas | Intermediários gravados em **disco** (lento) |
| | O **shuffle/sort** é caro e afeta diretamente o desempenho |

---

## 5. Spark × Hadoop

- **Spark:** framework distribuído **em memória** (mantém os dados na RAM entre as etapas). Arquitetura **Driver** (roda o `main`, escalona as tarefas) + **Executors** (executam as tarefas nos workers).
- **RDD:** coleção particionada entre os nós, processada em paralelo.

**Lista P2, Ex. 4 — recuperação de falhas** (gabarito resumido):

| | **Hadoop (HDFS/MapReduce)** | **Spark** |
|---|---|---|
| Diferença principal | Armazenamento em **disco** com **replicação física** | Execução **em memória** com **linhagem lógica** |
| Como recupera | Usa as **cópias dos blocos** em outros DataNodes (e as saídas intermediárias em disco) para reiniciar as tarefas perdidas | Usa o **DAG** das transformações para **recomputar em memória só as partições afetadas**, sem o custo de replicar e escrever em disco |

O Spark é bem mais rápido em **algoritmos iterativos** (ML), porque não lê e grava em disco a cada iteração.

---

## 6. Checklist

- [ ] Defino os 5 Vs
- [ ] Explico NameNode (gerencia, não armazena) × DataNode (armazena, roda o map)
- [ ] Resolvo um exercício de HDFS: blocos na ordem, bloco residual, replicação sem estourar capacidade nem repetir nó
- [ ] Resolvo um exercício de MapReduce com saída do map, entrada e saída de cada reducer
- [ ] Cito vantagens e desvantagens do MapReduce
- [ ] Explico a diferença de recuperação de falhas Hadoop × Spark
