package com.jpmc.midascore;

import com.jpmc.midascore.foundation.*;
import com.jpmc.midascore.entity.*;
import com.jpmc.midascore.repository.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Component
public class KafkaConsumer {

    private UserRepository userRepository;
    private TransactionRecordRepository transactionRecordRepository;
    private RestTemplate restTemplate;

    public KafkaConsumer(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository, RestTemplate restTemplate) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "transaction-group")
    public void listen(Transaction transaction) {
        System.out.println("Received transaction: " + transaction);

        // Fetch sender and recipient from the repository by ID
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        System.out.println("Users from service file: " + userRepository.findAll());
        System.out.println("Transaction sender id Works: " + transaction.getSenderId());
        System.out.println("Sender object: " + sender);

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            // If transaction is valid, record it
            TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
            transactionRecordRepository.save(transactionRecord);

            // Try and catch when calling the api
            float incentiveAmount = 0;
            try {
                incentiveAmount = getIncentiveApi(transaction).getAmount();
                System.out.println("Incentive amount: " + incentiveAmount);
            } catch
            (Exception e) {
                System.out.println("Error occurred while calling Incentive Api " + e);
            }

            // Update balance of sender
            sender.setBalance(sender.getBalance() - transaction.getAmount());

            // Update balance of recipient
            recipient.setBalance(recipient.getBalance() + transaction.getAmount()+incentiveAmount);

            // Persist updated users with their new balances
            userRepository.save(sender);
            userRepository.save(recipient);
        } else {
            // If not valid, discard the transaction (no changes to database)
            System.out.println("Transaction discarded: " + transaction);
        }
    }

    public Balance getBalance(Long userId) {

        Optional<UserRecord> user = userRepository.findById(userId);
        float finalBalance = 0;

        if (user.isPresent()) {
             finalBalance = user.get().getBalance();
            return new Balance(finalBalance);
        } else {
            return new Balance(finalBalance);
        }
    }

    public Incentive getIncentiveApi(Transaction transaction) {
        String url = "http://localhost:8080/incentive";
        return restTemplate.postForObject(url, transaction, Incentive.class);
    }
}
