package ExemploExecutorService;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.*;
import java.util.random.RandomGenerator;

class TaskRunnable implements Runnable {
    @Override
    public void run() {
        // Exibe o nome da thread atual para ver as threads do pool em ação
        String nomeThread = Thread.currentThread().getName();
        System.out.println("Executando na: " + nomeThread);
    }
}

class TaskCallable implements Callable<Integer>{

    @Override
    public Integer call() throws Exception {
        return RandomGenerator.getDefault().nextInt();
    }
}

public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //Cria um pool fixo com 3 threads
        ExecutorService executor = Executors.newFixedThreadPool(3);
        // Submete 6 tarefas para o pool
        /*for (int i = 0; i < 6; i++) {
            executor.submit(new TaskRunnable());
        }*/
        ArrayList<Future<Integer>> future = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            Future<Integer> callable_return = executor.submit(new TaskCallable());
            future.add(callable_return);
        }
//outras coisas
        for(int i =0; i< future.size(); i++){
            System.out.println("Valor do future" + future.get(i).get());
        }
        executor.shutdown();
    }
}