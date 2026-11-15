package ExemploExecutorService;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.ThreadLocalRandom;

public class BuscaMaiorValorTask extends RecursiveTask<Integer> {
    private static final int LIMIAR = 5_000_000;
    private final int[] array;
    private final int inicio;
    private final int fim;

    public BuscaMaiorValorTask(int[] array, int inicio, int fim) {
        this.array = array;
        this.inicio = inicio;
        this.fim = fim;
    }

    @Override
    protected Integer compute() {
        // Passo 3: Caso Base (Processamento Sequencial)
        if ((fim - inicio) <= LIMIAR) {
            int max = array[inicio];
            for (int i = inicio + 1; i < fim; i++) {
                if (array[i] > max) {
                    max = array[i];
                }
            }
            return max;
        }
        // Passo 4: Divisão em Subtarefas
        int meio = inicio + (fim - inicio) / 2;
        BuscaMaiorValorTask esquerda = new BuscaMaiorValorTask(array, inicio, meio);
        BuscaMaiorValorTask direita = new BuscaMaiorValorTask(array, meio, fim);
        // Dispara a tarefa da esquerda assincronamente
        esquerda.fork();
        // Processa a tarefa da direita na thread atual
        int maxDireita = direita.compute();
        // Aguarda a tarefa da esquerda e combina os resultados
        int maxEsquerda = esquerda.join();
        return Math.max(maxEsquerda, maxDireita);
    }

    public static void main(String[] args) {
        // Criando dados de teste
        int[] dados = new int[50_000_000];
        for (int i = 0; i < dados.length; i++) {
            dados[i] = ThreadLocalRandom.current().nextInt(1, 100_000_000);
        }
        // Passo 5: Execução no ForkJoinPool
        ForkJoinPool pool = ForkJoinPool.commonPool();
        BuscaMaiorValorTask tarefaPrincipal = new BuscaMaiorValorTask(dados, 0, dados.length);
        long tempoInicio = System.currentTimeMillis();
        int maiorValor = pool.invoke(tarefaPrincipal);
        long tempoFim = System.currentTimeMillis();
        System.out.println("Maior valor encontrado: " + maiorValor);
        System.out.println("Tempo de execução: " + (tempoFim - tempoInicio) + " ms");
    }
}