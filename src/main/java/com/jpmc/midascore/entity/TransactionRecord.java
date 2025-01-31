package com.jpmc.midascore.entity;

import jakarta.persistence.*;

import org.apache.catalina.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jpmc.midascore.entity.UserRecord;

@Entity
public class TransactionRecord {

    @Id
    @GeneratedValue()
    private long id;

    @ManyToOne
    @JoinColumn(name = "sender_id", nullable = false)
    private UserRecord senderUser;

    @ManyToOne
    @JoinColumn(name = "recipient_id", nullable = false)
    private UserRecord recipientUser;

    @Column(nullable = false)
    private float amount;

    protected TransactionRecord() {
    }

    public TransactionRecord(UserRecord senderUser, UserRecord recipientUser, float amount) {
        this.senderUser = senderUser;
        this.recipientUser = recipientUser;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return String.format("Transaction Record[id=%d, sendername=%d, senderamount='%f', recipientname=%d, recipientamount='%f", id, senderUser.getId(), senderUser.getBalance(), recipientUser.getId(), recipientUser.getBalance());
    }

    public Long getId() {
        return id;
    }

    public UserRecord getSenderUser() {
        return senderUser;
    }

    public UserRecord getRecipientUser() {
        return recipientUser;
    }

    public float getAmount() {
        return amount;
    }
    
}
