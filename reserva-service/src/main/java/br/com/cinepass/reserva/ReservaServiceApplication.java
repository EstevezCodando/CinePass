package br.com.cinepass.reserva;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ReservaServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(ReservaServiceApplication.class, args);
    }
}
