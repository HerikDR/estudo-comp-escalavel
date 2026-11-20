# Aula 07 — Granularidade e balanceamento de carga (versão P2)

> Aula de 25/09 · Profa. Larissa C. Shimomura
> **Filtro:** [12c](12c_orientacoes_prova2_revisao2.md). Só a **parte conceitual**: "o que é balanceamento estático, dinâmico, qual a vantagem de um, qual a desvantagem do outro".
> **Cortado desta versão** (não cai): todo o código Java (`Thread`, `ExecutorService`, `Callable`, `Future`, tipos de pool, `shutdown`), o exercício da soma com `Callable`, o treino de leitura de código, load balancer de servidores e localidade de dados em detalhe. A versão completa está no histórico do git.

---

## 1. Task, thread e chunk

| Termo | O que é |
|---|---|
| **Task** (tarefa) | Unidade de **trabalho** |
| **Chunk** | **Porção de dados** processada como uma unidade |
| **Thread** | **Recurso de execução**: quem executa as tasks |

Task ≠ thread. Normalmente há **mais tasks que threads** (ex.: 4 threads consumindo uma fila de 32 chunks).

---

## 2. Granularidade × número de threads

**Granularidade** = tamanho de cada unidade de trabalho.

| | Granularidade **grossa** (poucas tasks grandes, menos threads) | Granularidade **fina** (muitas tasks pequenas, mais threads) |
|---|---|---|
| Overhead (criação, escalonamento, sincronização) | Baixo | **Alto** |
| Balanceamento de carga | **Ruim**: risco de processadores ociosos | Bom |

- **Trade-off:** balanceamento × overhead. Ideal: tasks pequenas o bastante para ocupar todo o hardware e grandes o bastante para compensar o custo de gerenciá-las.
- **Nº de threads:** CPU-bound → ≈ nº de núcleos; I/O-bound → mais que o nº de núcleos (enquanto uma espera, outra usa a CPU).
- **O objetivo não é maximizar threads**, é maximizar o trabalho útil em paralelo, reduzindo ociosidade, sincronização, comunicação e troca de contexto.

**Exemplo da 1ª revisão** (tarefas A, B, C independentes, com A + B ≈ C):

| Threads | Distribuição | Resultado |
|---|---|---|
| 2 | T1: A depois B · T2: C | **Equilibrado**, terminam juntas |
| 3 | uma tarefa por thread | As threads de A e B ficam **ociosas** esperando C: mais fino, sem ganho |
| 4, quebrando C em C1 e C2 | — | Só ajuda se C1 e C2 forem **independentes** |

---

## 3. Balanceamento de carga: estático × dinâmico

| | **Estático** | **Dinâmico** |
|---|---|---|
| Quando a divisão é feita | **Antes** da execução (compilação/inicialização), em geral pelo programador | **Durante** a execução: quem termina pega a próxima tarefa |
| Tamanho do trabalho por thread | Fixo | Variável |
| **Vantagem** | **Baixo custo de sincronização/gerenciamento**; ótimo quando o custo das tarefas é conhecido e uniforme | **Adapta-se a custos diferentes**, melhor aproveitamento dos recursos |
| **Desvantagem** | Com carga **heterogênea**, gera **ociosidade**: o tempo total é o da **thread mais lenta** | **Custo de gerenciamento e coordenação**; tasks muito pequenas geram overhead |
| Use quando | Custo conhecido e aproximadamente uniforme | Custo variável/imprevisível ou problema recursivo |

**Exemplo do slide (8 tarefas, 4 threads):** P1–P4 baratas, P5–P8 caras. No estático (2 fixas por thread), as threads com P1–P4 ficam ociosas e o total é ditado pelas que pegaram as caras (100 ms). No dinâmico, quem termina pega a próxima da fila e o total cai (≈ 60 ms, supondo 10 ms e 50 ms por tarefa).

### Mapeamento estático (como dividir os dados)

| Forma | Ideia | Bom para |
|---|---|---|
| **Blocos** | Faixas contínuas, uma por thread (em matriz: linhas ou sub-blocos 2D) | Localidade; computação com vizinhos |
| **Cíclico / bloco-cíclico** | Elementos ou blocos distribuídos em **round robin** ("distribuir cartas") | Custo que **varia** ao longo dos dados |
| **Aleatório** | Blocos com elementos aleatórios | Evitar padrões que concentrem o custo |
| **Grafos** | Partições por região com o mínimo de arestas cortadas | Malhas e dados espaciais |

### Mapeamento dinâmico

- **Centralizado (Master/Slave):** um mestre mantém a fila e distribui tarefas. Fácil e balanceia sozinho, mas o mestre vira **gargalo**.
- **Distribuído (*work stealing*):** cada thread tem a **sua fila** e, quando ela esvazia, **rouba** tarefas de outra. É **dinâmico** e é o algoritmo do **Fork/Join** do Java.

---

## 4. Checklist

- [ ] Explico o trade-off granularidade × nº de threads (balanceamento × overhead)
- [ ] Explico estático × dinâmico com **vantagem e desvantagem** de cada um
- [ ] Sei por que, no estático, o tempo total é o da thread mais lenta
- [ ] Diferencio blocos, cíclico e aleatório e sei quando o cíclico ajuda
- [ ] Explico Master/Slave (gargalo) e *work stealing* (dinâmico, Fork/Join)
