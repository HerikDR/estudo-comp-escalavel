# K-Means e K-Means paralelo — guia de estudo

> Base: fichamentos da revisão de literatura (slots KM1–KM4, 10 artigos lidos na íntegra).
> Estrutura: **(1)** fundamentos do k-means (revisão rápida) → **(2)** o que muda ao paralelizar →
> **(3)** famílias de algoritmos da literatura → **(4)** como medir → **(5)** lacunas e ressalvas → **(6)** cola.
>
> **Convenção de fontes.** Onde há `(Autor ano, p. N)` o dado vem do fichamento. Itens marcados
> **[geral]** são conhecimento padrão de k-means que não vem diretamente dos artigos; itens marcados
> **[inferência]** são deduções minhas a partir dos fichamentos. As conferências por integrante do grupo
> ainda estão pendentes nos fichamentos: confira no PDF antes de citar no relatório.

---

## 1. Fundamentos do k-means (revisão rápida)

### 1.1 O problema

Dados *n* pontos em ℝᵈ e um número *k* fixo, particionar em *k* grupos minimizando a variância
intra-classe (soma dos quadrados das distâncias de cada ponto ao centro do seu grupo):

```
φ(C) = Σ_x  min_{c ∈ C} ‖x − c‖²
```

- Resolver exatamente é **NP-difícil**, mesmo com 2 grupos (Arthur & Vassilvitskii 2007, p. 1).
- O k-means é uma **heurística de busca local**: converge para um ótimo **local**, que pode ser muito
  ruim (Arthur & Vassilvitskii 2007, p. 2).

### 1.2 O algoritmo (iteração de Lloyd, em lote)

```
1. Escolher k centros iniciais C
2. REPETIR até convergir:
     a) ATRIBUIÇÃO:   cada ponto vai para o centro mais próximo           → custo ~ n·k·d
     b) ATUALIZAÇÃO:  cada centro vira a média dos pontos do seu grupo    → custo ~ n·d
     c) PARADA:       centros param de mudar / MSE estabiliza / máx. iterações
```

Por que funciona: são as duas **condições necessárias** de Lloyd, aplicadas alternadamente.
(i) dado o particionamento, o melhor representante de cada grupo é o **centro de massa**;
(ii) dados os representantes, a melhor partição é a do **vizinho mais próximo** (fronteira no ponto médio).
A cada passo o erro **não aumenta** (Lloyd 1982, p. 131–132) → termina em número finito de passos, pois há
número finito de partições (MacQueen 1967, p. 294).

### 1.3 Custo

- Por iteração: ≈ **3·n·k·d** flops, dominado pelas distâncias quando *n* é grande
  (Dhillon & Modha 2000, §3.2, p. 6). Total ≈ 3·n·k·d·**I**, com *I* = nº de iterações.
- "O cálculo mais intenso é o das distâncias" (Zhao et al. 2009, p. 675).
- Terminação é garantida, mas o **tempo pode ser exponencial no pior caso** (Bahmani et al. 2012, p. 622).
  Na prática *I* é pequeno e **muda muito com o conjunto** (3 a 164 iterações em Dhillon & Modha, Fig. 3)
  e com a inicialização.

### 1.4 Limitações que atravessam todos os artigos

| Limitação | Onde aparece |
|---|---|
| Converge a ótimo **local**, não global | MacQueen p. 282; Lloyd p. 131; Arthur & V. p. 2; Bahmani p. 622 |
| Sensível ao ponto de partida (e, na versão sequencial, à ordem dos dados) | MacQueen p. 290, 294 |
| *k* precisa ser fornecido | **[geral]** |
| Pressupõe grupos "esféricos" de tamanho parecido (distância euclidiana à média) | **[geral]** |

### 1.5 Inicialização (afeta qualidade **e** nº de iterações)

| Método | Ideia | Garantia / efeito |
|---|---|---|
| **Aleatória** | k pontos sorteados uniformemente | Sem garantia; pior caso arbitrariamente ruim |
| **k-means++** (Arthur & V. 2007) | 1º centro uniforme; cada próximo escolhido com prob. **D(x)²/ΣD²** (D = distância ao centro mais próximo já escolhido) | E[φ] ≤ **8(ln k + 2)·φ_OPT** (O(log k)-competitivo). Reduz iterações de Lloyd e tempo total na maioria dos casos |
| **k-means‖** (Bahmani et al. 2012) | versão **paralela** do k-means++ (ver §3.2) | O(log k) com k-means++ no passo final |

Dados úteis: no Spam, iterações de Lloyd até convergir (média de 10 execuções), k = 20/50/100:
Random 176,4 / 166,8 / 60,4; k-means++ 38,3 / 42,2 / 36,6; k-means‖ (ℓ=2k, r=5) 23,3 / 28,1 / 29,7
(Bahmani, Tabela 6, p. 628).

### 1.6 Três "k-means" que a literatura mistura (cuidado ao citar)

1. **MacQueen 1967** — procedimento **sequencial/online**: cada ponto novo entra no grupo de média mais
   próxima e a média é atualizada com peso. Depende da ordem dos dados.
2. **Dois passos em lote** (MacQueen §3.6, atribuído a Forgy e Jennrich) — é o laço acima. Os artigos
   posteriores chamam de **"iteração/algoritmo de Lloyd"**. **É este que se paraleliza.**
3. **Lloyd 1957/1982** — quantização **escalar** (1-D, PCM) com as mesmas duas condições. Não trata de
   pontos em ℝᵈ; chamar o laço de "Lloyd" é **atribuição por tradição**.

> Para o relatório: dizer "iteração de Lloyd" e ancorar a descrição em Arthur & V. (2007, §2.1) ou
> MacQueen (1967, §3.6).

---

## 2. K-means paralelo vs. K-means "normal"

### 2.1 Onde está (e onde não está) o paralelismo

| Etapa | Paralelizável? | Por quê |
|---|---|---|
| Distância ponto→centros e atribuição | **Sim, trivialmente** | "Cada ponto decide independentemente qual centro é mais próximo" (Bahmani p. 622); as distâncias de um objeto independem das dos outros (Zhao p. 675) |
| Soma de pontos por centro | **Sim, parcialmente** | Cada trabalhador soma localmente; depois combina-se (soma é associativa) |
| Cálculo do novo centro | Barato, após a **redução global** | média = soma total / contagem total |
| **Laço de iterações** | **Não** | Os novos centros são necessários na iteração seguinte: "os procedimentos iterativos devem ser executados em série" (Zhao p. 675) |
| **Inicialização k-means++** | **Não (ingênua)** | "Inerentemente sequencial": o i-ésimo centro depende dos i−1 anteriores → **k passes** sobre os dados (Bahmani p. 623) |

### 2.2 A decomposição-padrão (a mesma nos três artigos de paralelização exata)

```
Particionar os n pontos em P blocos fixos de ~n/P pontos (um por trabalhador).
A cada iteração:
   [ fase assíncrona ]  cada trabalhador, SÓ nos seus pontos:
                          - atribui cada ponto ao centro mais próximo
                          - acumula, por centro: soma vetorial, contagem (e erro quadrático)
   [ BARREIRA / redução global ]  combina as somas e contagens parciais de todos
   [ todos ] novo centro = soma / contagem;  decidem a parada (MSE)  → novos centros a todos
```

(Dhillon & Modha p. 5–7: "uma fase de computação assíncrona seguida de uma fase de comunicação síncrona").

### 2.3 Diferenças em relação ao sequencial

| Aspecto | K-means sequencial | K-means paralelo (exato) |
|---|---|---|
| **Resultado** | referência | **Idêntico** ao sequencial dado o mesmo ponto de partida: "a solução é a mesma da versão sequencial do Lloyd" (Ene et al. p. 16). Dhillon & Modha fixam centróides iniciais idênticos para qualquer P (p. 9). *[inferência: em ponto flutuante, a ordem da soma pode causar diferenças mínimas]* |
| **Dados** | um só espaço de memória | particionados; cada trabalhador vê só n/P pontos |
| **Estado compartilhado** | — | apenas os **k centros** (k·d valores), conhecidos por todos |
| **Comunicação/sincronização** | nenhuma | **1 redução global por iteração**, sobre k·d (+k+1) valores — **independe de n**. Dhillon & Modha agrupam as 2k+1 reduções numa só sobre um bloco contíguo (p. 7) |
| **Custo por iteração** | ≈ 3nkd·T_flop | ≈ 3nkd·T_flop / P  **+**  d·k·T_reduce(P), com T_reduce(P) = O(log P) (Dhillon & Modha p. 6–8) |
| **Parte serial** | quase tudo é "serial" | atualização dos centros, redução, **inicialização**, leitura de dados e o laço externo. *[inferência: é o que limita o speedup, à la Amdahl]* |
| **Número de iterações I** | I | **o mesmo** (se mesmo início e paralelização exata) → o speedup vem do custo **por iteração** |
| **Quando compensa** | — | Comunicação desprezível se `P·T_reduce(P) / (3·T_flop) ≪ n` (condição (7), p. 8) — **só depende de n**, não de d nem de k. Quanto maior n, mais perto do speedup linear |

### 2.4 Exato × aproximado (distinção central)

- **Paralelização exata**: faz o *mesmo* cálculo, só dividido (Dhillon & Modha; Zhao et al.; Parallel-Lloyd).
  Mesma qualidade; o ganho é limitado por comunicação e parte serial.
- **Aproximada**: *muda o algoritmo* para trabalhar menos (amostragem de Ene et al.; mini-lote de Sculley).
  Muito mais rápida, mas **troca qualidade por tempo**.

---

## 3. Famílias de algoritmos desenvolvidos (literatura fichada)

Mapa geral:

```
K-means paralelo / escalável
├── A. Paralelização EXATA do laço de Lloyd (paralelismo de dados)
│     ├── A1. Memória distribuída, troca de mensagens (MPI)  ........ Dhillon & Modha 2000
│     ├── A2. MapReduce / Hadoop (map + combine + reduce)  .......... Zhao et al. 2009 (PKMeans)
│     └── A3. "Parallel-Lloyd" em MapReduce (baseline) .............. Ene, Im & Moseley 2011
├── B. Paralelização da INICIALIZAÇÃO ............................. Bahmani et al. 2012 (k-means‖)
├── C. Escala por APROXIMAÇÃO (muda o algoritmo)
│     ├── C1. Amostragem + agrupar a amostra  ....................... Ene, Im & Moseley 2011
│     └── C2. Mini-lotes (SGD-like por centro)  ..................... Sculley 2010
└── D. Plataforma / biblioteca ..................................... Meng et al. 2016 (MLlib/Spark)
```

### 3.A1 — Dhillon & Modha (2000): k-means em MPI (memória distribuída)

- **Modelo:** SPMD com MPI; máquinas *shared-nothing*. Considerado o **modelo-base** de paralelização.
- **Algoritmo:** processo 0 escolhe centróides iniciais e faz `MPI_Bcast`; cada um dos P processos fica com
  n/P pontos; calcula distâncias, acumula (somas, contagens, erro) por centróide; `MPI_Allreduce` a cada
  iteração; todos recalculam centróides e decidem a parada de forma independente (MSE).
- **Teoria:** `T_P ∼ 3nkd·I·T_flop/P + d·k·I·T_reduce(P)`; speedup/scaleup → ótimo quando n cresce.
- **Experimentos:** IBM SP2, até 16 nós (POWER2 160 MHz, 256 MB); dados sintéticos com 8 grupos; tempos
  **sem leitura de disco**; centróides iniciais idênticos para qualquer P.
  - Variando n (2¹³ a 2²¹ pontos; d = k = 8): speedup em 16 proc. = **15,62** (maior) e **6,22** (menor).
  - Variando d ou k: speedups ≈ 15,1–15,7 → **independentes de d e de k**.
  - Scaleup: tempo por iteração ≈ constante; maior conjunto (2²⁵ pontos, ≈ 2 GB) não cabe num nó.
- **Memória compartilhada:** dizem que é "facilmente adaptado", com cópia separada dos centróides por
  *thread* — **sem implementar nem medir** (p. 14–15).
- **Ressalvas:** dados sintéticos, sem I/O, I variando muito (3–164), só curvas para valores intermediários.
  O "2¹¹" do texto é erro de grafia (adotou-se 2¹³).

### 3.A2 — Zhao, Ma & He (2009): PKMeans em MapReduce/Hadoop

- **Mapeamento do algoritmo:**
  - **map:** atribui cada amostra ao centro mais próximo (variável global `centers`) → emite (índice do centro, amostra).
  - **combine:** ao fim de cada tarefa map, soma parcialmente as amostras do mesmo centro e conta → **reduz tráfego de rede**.
  - **reduce:** soma as parciais de todos os nós e divide pela contagem → novos centros.
- **Teoria de base:** mesma de Dhillon & Modha (distâncias independentes; iterações em série), embora **não os cite**.
- **Experimentos:** Hadoop 0.17.0, nós de 2 núcleos a 2,8 GHz/4 GB, **1 a 4 computadores**, conjuntos de 1–8 GB;
  mede speedup, scaleup e **sizeup** — "muito bom", **só em curvas**.
- **Ressalvas:** dataset **não descrito** (nº de pontos, d, k); nenhum valor numérico no texto; não descreve o
  laço externo nem o custo de iniciar um *job* Hadoop por iteração.

### 3.A3 — Ene, Im & Moseley (2011): "Parallel-Lloyd" (baseline)

- Mesmo esquema: partição uniforme dos pontos, envio dos k centros a cada máquina, médias e contagens locais,
  agregação. É a **única ficha que afirma explicitamente** que o resultado é igual ao sequencial.
- ⚠ **O artigo não é sobre k-means** (é k-center/k-median); o Lloyd paralelo é só *baseline*.

### 3.B — Bahmani et al. (2012): k-means‖ (inicialização paralela)

- **Problema:** k-means++ faz **k passes** sobre os dados. Lloyd é fácil de paralelizar, a inicialização não.
- **Algoritmo:** começa com 1 centro uniforme; em **O(log ψ) rodadas**, amostra **cada ponto
  independentemente** com prob. `ℓ·d²(x,C)/φ_X(C)` (ℓ = Ω(k)) e adiciona a C (~ℓ pontos/rodada); no fim,
  pesa cada candidato pelo nº de pontos que lhe são mais próximos e **reagrupa** os ~ℓ·log ψ candidatos em k
  centros (p. ex. com k-means++ numa só máquina).
  - Diferença-chave: k-means++ escolhe **1 centro por passo**; k-means‖ escolhe **~ℓ por rodada, em paralelo**.
- **Garantia:** O(α)-aproximação (O(log k) com k-means++ no passo final).
- **MapReduce:** cada mapper amostra independentemente; φ_X(C) = soma dos custos parciais no reducer;
  supõe C pequeno o bastante para caber em memória / ser difundido.
- **Resultados:** custo final comparável ao k-means++ e muito menor que Random; **menos iterações de Lloyd**.
  KDDCup1999 (4,8 mi pontos), cluster Hadoop de 1.968 nós, tempo em min (k = 500 / 1000):
  Random 300,0 / 489,4; Partition 420,2 / 1.021,7; k-means‖ (ℓ=0,5k) 69,0 / 46,2.
  Rodadas: precisa de r·ℓ ≥ k; ponto ótimo experimental r ≈ 8.
- **Ressalvas:** escala linear com nº de máquinas é **afirmada, não medida** (nenhum experimento varia nós);
  Random limitado a 20 iterações no paralelo; **não propõe nada novo para as fases pós-inicialização**.

### 3.C1 — Ene, Im & Moseley (2011): amostragem

- **Iterative-Sample:** amostra aleatória, descarta pontos "bem representados", repete; agrupa
  (amostra + resto) em **uma máquina** com Lloyd/busca local. No k-median cada ponto da amostra recebe peso.
- **Garantia:** aproximação de fator constante, O(1/δ) rodadas de MapReduce, memória O(k²nᵟ).
- **Resultados (tempo s / custo relativo ao Parallel-Lloyd), k-median, R³, k = 25:**

  | Pontos | Parallel-Lloyd | Sampling-Lloyd | Custo Sampling-Lloyd |
  |---|---|---|---|
  | 1 mi | 205,7 | 4,0 | 1,132 |
  | 2 mi | 458,0 | 8,0 | 1,064 |
  | 5 mi | 1.333,3 | 18,3 | 1,106 |
  | 10 mi | 702,3 | 38,0 | 1,073 |

  Ganho de **18× a 73×** (o texto diz "~20×"); custo **5% a 16,5% pior** (o texto diz "pequeno").
- **Ressalvas:** tudo em **uma só máquina, MapReduce simulado, sem custo de comunicação**; dados sintéticos
  em R³; k-means só como "evidência preliminar" (p. 18); tempo não monótono em n sem explicação.

### 3.C2 — Sculley (2010): mini-batch k-means

- **Motivação:** lote custa O(k·n·s) por iteração; SGD *online* (Bottou & Bengio) é rápido mas dá soluções
  piores por ruído estocástico.
- **Algoritmo:** a cada iteração sorteia **b exemplos**, acha o centro mais próximo de cada um e atualiza cada
  centro com **taxa de aprendizado por centro η = 1/v[c]** (v[c] = contagem acumulada do centro).
  Centros iniciais = k exemplos sorteados.
- **Resultados (RCV1, 781k exemplos; b = 1000; k = 3, 10, 50):** quase ótimo **ordens de grandeza mais rápido**
  que o lote e melhor que SGD.
- **Ressalvas:** pôster de 2 páginas, **sequencial**, texto esparso, resultados só em curvas, resultado
  **não é idêntico** ao do lote. A 2ª contribuição (centros esparsos por projeção L1) não importa para dados
  densos de baixa dimensão.

### 3.D — Meng et al. (2016): MLlib / Spark

- Biblioteca de ML distribuída do Spark; k-means aparece **uma única vez**, numa lista de métodos.
- Dado citável: ganho entre versões MLlib 1.0→1.1, KMeans **2,5×** (Fig. 2b); média de 3× em todos os
  algoritmos — **é speedup entre versões, não entre nº de nós**.
- ⚠ **Não descreve** o algoritmo de k-means do MLlib (inicialização, paralelização, broadcast de centróides),
  nem usa k-means‖. Serve só como **contexto de plataforma**, não como fonte do algoritmo.

### 3.E — Quadro comparativo

| Artigo | Família | Modelo de execução | Resultado = sequencial? | Mede escalabilidade? | Peso p/ nosso estudo |
|---|---|---|---|---|---|
| Dhillon & Modha 2000 | A1 exato | MPI, memória distribuída (SP2, 16 nós) | Sim (centróides iniciais fixos) | **Sim, numérico** (speedup 6,22–15,62; scaleup) | **Central** |
| Zhao et al. 2009 | A2 exato | MapReduce/Hadoop (1–4 PCs) | Sim (mesma lógica) | Só curvas, sem valores, dataset não descrito | Alto (tradução p/ MapReduce) |
| Ene et al. 2011 (Parallel-Lloyd) | A3 exato | MapReduce **simulado** em 1 máquina | **Sim, afirmado** (p. 16) | Tempo simulado, sem comunicação | Contexto |
| Bahmani et al. 2012 | B (inicialização) | MapReduce/Hadoop, 1.968 nós | n/a (muda só a semeadura) | Compara algoritmos em cluster fixo; escala linear só afirmada | Médio |
| Ene et al. 2011 (Sampling) | C1 aproximado | MapReduce simulado | **Não** (+5% a +16,5% de custo) | Simulado | Contexto |
| Sculley 2010 | C2 aproximado | Sequencial | **Não** (quase ótimo) | Tempo × erro, só curvas | Contexto |
| Meng et al. 2016 | D plataforma | Spark | — | Só ganho entre versões | Baixo |

---

## 4. Como a literatura mede escalabilidade

Definições (Dhillon & Modha, p. 9 e 11–12):

- **Speedup relativo** = tempo com **1** processador / tempo com **P** (mesmo conjunto de dados, mesmo k).
- **Scaleup relativo** = tempo **por iteração** com n pontos em 1 proc. / tempo por iteração com **n·P** pontos
  em P proc. (ideal = 1, tempo constante).
- **Sizeup** (Zhao et al.) = nº de máquinas fixo, conjunto crescente.

Boas práticas observadas:

1. **Fixar os centróides iniciais** para qualquer P — senão *I* varia e o tempo total não é comparável.
2. Usar **tempo por iteração** no scaleup, porque *I* muda com o conjunto.
3. Declarar o que fica fora do tempo (Dhillon & Modha ignoram I/O; Ene et al. ignoram comunicação).
4. Média de várias execuções (Dhillon & Modha: 5 medidas por ponto).

---

## 5. Lacunas e ressalvas (importante para não superafirmar)

- **O único speedup numérico de k-means paralelo nos artigos lidos é o de Dhillon & Modha (MPI).** Zhao et al.
  só têm curvas; Ene et al. é simulado; Bahmani et al. afirma sem medir.
- **Memória compartilhada / threads: lacuna.** Dhillon & Modha descrevem a adaptação em uma frase (p. 14–15)
  sem medir; nenhum outro artigo do slot mede speedup por threads. É a lacuna mais relevante para um cenário
  de **uma máquina** (ex.: Java com `ExecutorService`/`ForkJoin`, até 150.000 pontos).
- **O que a teoria prevê para memória compartilhada [inferência]:** a redução global vira a combinação de
  acumuladores por thread (cada thread com sua cópia de somas/contagens, depois fusão); não há rede, mas ainda
  há barreira por iteração, parte serial e possíveis efeitos de memória/cache. A condição `P·T_reduce(P)/(3·T_flop) ≪ n`
  sugere que n = 150.000 é grande para poucas threads, mas a fusão e a barreira ainda custam.
- **Ene et al. (2011) não é sobre k-means** (k-median/k-center; "coresets" não aparece no texto; não cita Dhillon & Modha nem Zhao et al.).
- **Meng et al. (2016) não descreve** o k-means do MLlib; a ligação MLlib ↔ k-means‖ **não** está nos artigos.
- **Garantia de qualidade ≠ garantia de ótimo:** a paralelização exata preserva o resultado do sequencial,
  **inclusive suas limitações** (ótimo local, sensibilidade à inicialização). Quem melhora qualidade é a
  inicialização (k-means++/‖), não o paralelismo.
- **Divergências texto × tabela já resolvidas nos fichamentos** (valeu o dado observado): Dhillon & Modha
  (menor conjunto = 2¹³, não 2¹¹); Ene et al. (ganho 18×–73×, não "~20×"; perda 5%–16,5%, não "desprezível").
- O Arthur & Vassilvitskii é **manuscrito do autor** (citar por seção/teorema, não só por página do PDF).

---

## 6. Cola rápida (para revisar antes da prova/apresentação)

**Em uma frase:** o k-means em lote paraleliza bem porque a atribuição é independente por ponto e a atualização é
uma redução (soma/contagem); o que resta serial é o laço de iterações e a inicialização.

**Receita do k-means paralelo exato:** particionar pontos → centros em todos → atribuir localmente + somas
parciais → redução global (k·d valores) → novos centros → repetir.

**Mesma ideia, três vocabulários:**

| Conceito | MPI (Dhillon & Modha) | MapReduce (Zhao et al.) | Spark/outros |
|---|---|---|---|
| Distribuir centros | `MPI_Bcast` | variável global `centers` (lida por todos os mappers) | *broadcast* (geral, p. 4 de Meng et al.) |
| Atribuição + soma parcial | laço local no bloco | **map** (+ **combine**) | — |
| Combinar | `MPI_Allreduce` | **reduce** | agregação em árvore (geral) |

**Perguntas de revisão:**

1. Por que o resultado do k-means paralelo exato é igual ao sequencial? O que precisa ser fixado para a comparação ser justa?
2. O que cresce com *n*, o que cresce com *k·d*, e o que cresce com *P* no custo `3nkd·I·T/P + dkI·T_reduce(P)`?
3. Por que o speedup de Dhillon & Modha é pior (6,22) para o menor conjunto que para o maior (15,62)?
4. Qual a parte "inerentemente sequencial" do k-means++ e como o k-means‖ a contorna?
5. Qual a diferença entre paralelização **exata** e **aproximada**? Dê um exemplo de cada e o que se perde.
6. Por que o scaleup usa tempo **por iteração**?
7. O que o *combiner* do PKMeans faz e por que reduz tráfego?
8. Por que não se pode afirmar, com base nos artigos lidos, que o k-means escala em *threads* de memória compartilhada?

**Números para lembrar:**

- Custo/iteração ≈ **3·n·k·d** flops (Dhillon & Modha).
- k-means++ → E[φ] ≤ **8(ln k + 2)·φ_OPT**.
- Speedup (16 proc., SP2): **15,62** (n = 2²¹) vs **6,22** (n = 2¹³).
- k-means‖: **O(log ψ)** rodadas, r ≈ **8** na prática; r·ℓ ≥ k.
- Sampling-Lloyd (Ene): **18×–73×** mais rápido, **+5% a +16,5%** de custo (k-median, simulado).
- Mini-batch (Sculley): b = **1000**, η = **1/v[c]**.

---

## Apêndice — Referências (as 10 fichadas para o k-means)

- MacQueen, J. Some methods for classification and analysis of multivariate observations. *Proc. 5th Berkeley Symp.*, v. 1, p. 281–297, 1967.
- Lloyd, S. P. Least squares quantization in PCM. *IEEE Trans. Inf. Theory*, IT-28(2), p. 129–137, 1982.
- Arthur, D.; Vassilvitskii, S. k-means++: the advantages of careful seeding. *SODA '07*, p. 1027–1035, 2007.
- Bahmani, B.; Moseley, B.; Vattani, A.; Kumar, R.; Vassilvitskii, S. Scalable k-means++. *PVLDB*, 5(7), p. 622–633, 2012.
- Dhillon, I. S.; Modha, D. S. A data-clustering algorithm on distributed memory multiprocessors. *Large-Scale Parallel Data Mining*, LNCS 1759, p. 245–260, 2000.
- Zhao, W.; Ma, H.; He, Q. Parallel k-means clustering based on MapReduce. *CloudCom 2009*, LNCS 5931, p. 674–679.
- Ene, A.; Im, S.; Moseley, B. Fast clustering using MapReduce. *KDD '11*, p. 681–689, 2011.
- Sculley, D. Web-scale k-means clustering. *WWW '10*, p. 1177–1178, 2010.
- Meng, X. et al. MLlib: machine learning in Apache Spark. *JMLR*, 17, p. 1–7, 2016.
