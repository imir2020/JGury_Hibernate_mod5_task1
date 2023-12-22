package by.itacademy.hibernate.entity;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import java.io.Serializable;


public interface BaseEntity<T extends Serializable> {
    T getId();

    void setId(T id);
}
