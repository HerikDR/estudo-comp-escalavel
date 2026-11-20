# Aula 09b — Programação em Scala (versão P2)

> Aula de 21/10 · Profa. Larissa C. Shimomura
> **Filtro:** [12c](12c_orientacoes_prova2_revisao2.md). **Cai código, mas só Spark** (e o código Spark da disciplina é escrito em **Scala**): "o máximo que eu vou colocar é o Spark". **Não cai:** Java, nem exercícios de **Scala puro** como os da aula (coleções, `case class`): "eu não vou cobrar". O código Spark aparece para **ler** (desenhar o DAG, traduzir para álgebra), não para escrever.
> Por isso esta versão guarda **só o mínimo de sintaxe para ler um código Spark** (DAG e DataFrame → álgebra). Os conceitos de programação funcional estão em [09, §5](09_nosql_cap_spark_rdd.md#5-programação-funcional-só-a-ideia).
> **Cortado:** `class`/`case class`/`trait`, `Option`, coleções mutáveis, os 4 exercícios da aula, o WordCount com coleções e todo o treino de leitura. A versão completa está no histórico do git.

---

## Scala mínimo para ler código Spark

| Sintaxe | Significado | Exemplo |
|---|---|---|
| `val x = ...` | Valor **imutável** (não pode ser reatribuído) | `val dados = sc.parallelize(...)` |
| `x => expr` | **Lambda** (função anônima): entrada `x`, saída `expr` | `filter(par => par._2 >= 10)` |
| `(a, b) => a + b` | Lambda de dois parâmetros | `reduceByKey((v1, v2) => v1 + v2)` |
| `_ + _` | Atalho: cada `_` é um parâmetro, na ordem | `reduceByKey(_ + _)` = `(a, b) => a + b` |
| `("A", 10)` | **Tupla / par chave-valor** | `Seq(("A",10), ("B",20))` |
| `par._1`, `par._2` | 1º e 2º elementos da tupla ("ponto underline 1/2") | em `("A",10)`: `_1 = "A"`, `_2 = 10` |
| `linha.split(";")(1)` | Quebra a string e pega o **2º** pedaço (índice com parênteses, começa em 0) | `"a;b;c".split(";")(1)` = `"b"` |
| `.metodo1().metodo2()` | Encadeamento: cada chamada gera um novo RDD/DataFrame | `linhas.filter(...).map(...)` |

**DataFrame (Spark SQL):** `$"coluna"` refere-se a uma coluna; `===` é a igualdade em condições (`$"cidade" === "Londrina"`); `&&` é o "e" lógico (vira **∧** na álgebra). Ver [10, §3](10_spark_pratica_sparksql.md).

**`map` × `flatMap`:** `map` gera **um** elemento de saída por entrada; `flatMap` pode gerar **vários** e "achata" o resultado (ex.: frase → palavras).
