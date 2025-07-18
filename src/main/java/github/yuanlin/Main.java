package github.yuanlin;

import github.yuanlin.coordinator.BenchmarkCoordinator;

import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java BenchmarkMain <config-file> [config-file2] ...");
            System.exit(1);
        }

        try {
            // 创建协调器
            BenchmarkCoordinator coordinator = new BenchmarkCoordinator(8085);

            if (args.length == 1) {
                // 单个测试
                coordinator.startBenchmark(args[0]);
            } else {
                // 多个测试
                List<String> configFiles = Arrays.asList(args);
                coordinator.startBenchmarks(configFiles);
            }

            logger.info("Benchmark completed successfully");

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Benchmark failed", e);
            System.exit(1);
        }
    }

}
