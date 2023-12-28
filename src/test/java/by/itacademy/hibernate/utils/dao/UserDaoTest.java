package by.itacademy.hibernate.utils.dao;


import by.itacademy.hibernate.dao.UserDao;
import by.itacademy.hibernate.entity.Birthday;
import by.itacademy.hibernate.entity.Company;
import by.itacademy.hibernate.utils.TestDataImporter;
import by.itacademy.hibernate.entity.Payment;
import by.itacademy.hibernate.entity.User;
import by.itacademy.hibernate.util.HibernateUtil;
import com.querydsl.core.Tuple;
import lombok.Cleanup;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class UserDaoTest {

    private final SessionFactory sessionFactory = HibernateUtil.buildSessionFactory();
    private final UserDao userDao = UserDao.getInstance();

    @BeforeAll
    public void initDb() {
        TestDataImporter.importData(sessionFactory);
    }

    @AfterAll
    public void finish() {
        sessionFactory.close();
    }

    @Test
    void findAll() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<User> results = userDao.findAll(session);
        assertThat(results).hasSize(5);

        List<String> fullNames = results.stream().map(User::fullName).collect(toList());
        assertThat(fullNames).containsExactlyInAnyOrder("Bill Gates", "Steve Jobs", "Sergey Brin", "Tim Cook", "Diane Greene");

        session.getTransaction().commit();
    }

    @Test
    void findAllByFirstName() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<User> results = userDao.findAllByFirstName(session, "Bill");

        assertThat(results).hasSize(1);
        assertThat(results.get(0).fullName()).isEqualTo("Bill Gates");

        session.getTransaction().commit();
    }

    @Test
    void findLimitedUsersOrderedByBirthday() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        int limit = 3;
        List<User> results = userDao.findLimitedUsersOrderedByBirthday(session, limit);
        assertThat(results).hasSize(limit);

        List<String> fullNames = results.stream().map(User::fullName).collect(toList());
        assertThat(fullNames).contains("Diane Greene", "Steve Jobs", "Bill Gates");

        session.getTransaction().commit();
    }

    @Test
    void findAllByCompanyName() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<User> results = userDao.findAllByCompanyName(session, "Google");
        assertThat(results).hasSize(2);

        List<String> fullNames = results.stream().map(User::fullName).collect(toList());
        assertThat(fullNames).containsExactlyInAnyOrder("Sergey Brin", "Diane Greene");

        session.getTransaction().commit();
    }

    @Test
    void findAllPaymentsByCompanyName() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<Payment> applePayments = userDao.findAllPaymentsByCompanyName(session, "Apple");
        assertThat(applePayments).hasSize(5);

        List<Integer> amounts = applePayments.stream().map(Payment::getAmount).collect(toList());
        assertThat(amounts).contains(250, 500, 600, 300, 400);

        session.getTransaction().commit();
    }

    @Test
    void findAveragePaymentAmountByFirstAndLastNames() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        Double averagePaymentAmount = userDao.findAveragePaymentAmountByFirstAndLastNames(session, "Bill", "Gates");
        assertThat(averagePaymentAmount).isEqualTo(300.0);

        session.getTransaction().commit();
    }

    @Test
    void findCompanyNamesWithAvgUserPaymentsOrderedByCompanyName() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<Tuple> results = userDao.findCompanyNamesWithAvgUserPaymentsOrderedByCompanyName(session);
        assertThat(results).hasSize(3);

        List<String> orgNames = results.stream().map(a -> a.get(0,String.class)).collect(toList());
        assertThat(orgNames).contains("Apple", "Google", "Microsoft");

        List<Double> orgAvgPayments = results.stream().map(a -> a.get(1,Double.class)).collect(toList());
        assertThat(orgAvgPayments).contains(410.0, 400.0, 300.0);

        session.getTransaction().commit();
    }

    @Test
    void isItPossible() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<Tuple> results = userDao.isItPossible(session);
        assertThat(results).hasSize(2);

        List<String> names = results.stream().map(r -> (r.get(0,User.class)).fullName()).collect(toList());
        assertThat(names).contains("Sergey Brin", "Steve Jobs");

        List<Double> averagePayments = results.stream().map(r -> r.get(1,Double.class)).collect(toList());
        assertThat(averagePayments).contains(500.0, 450.0);

        session.getTransaction().commit();
    }
    @Test
    void usersAvgPayments() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();
        List<Tuple> result = userDao.usersAvgPayments(session);
        List<String> users = result.stream().map(r -> (r.get(0, User.class)).fullName()).toList();

        List<Double> avgPayments = result.stream().map(r -> r.get(1, Double.class)).toList();
        assertEquals(users.size(), avgPayments.size());
        session.getTransaction().commit();
    }

    @Test
    void paymentsLessUserAvgPayment() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();
        List<Tuple> result = userDao.paymentsLessUserAvgPayment(session);
        List<String> fullNames = result.stream().map(r -> (r.get(0, User.class)).fullName()).toList();
        List<Double> avgPayments = result.stream().map(r -> r.get(1, Double.class)).toList();

        fullNames.forEach(System.out::println);
        avgPayments.forEach(System.out::println);
        assertTrue(avgPayments.size() != 0);
        session.getTransaction().commit();
    }

    @Test
    void userWithLastNameAndPayments() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();
        List<Payment> results = userDao.userWithLastNameAndPayments(session, "Gates");
        assertThat(results).hasSize(3);
        List<Integer> amounts = results.stream().map(Payment::getAmount).toList();
        assertThat(amounts).contains(100, 300, 500);
        session.getTransaction().commit();
    }

    @Test
    void usersOrderedByAvgPayments() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();
        List<User> result = userDao.usersOrderedByAvgPayments(session);

        List<String> fullNames = result.stream().map(User::fullName).toList();
        fullNames.forEach(System.out::println);
        assertTrue(fullNames.size() != 0);

        session.getTransaction().commit();
    }

    @Test
    void companyNameBirthDayUsers() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();
        List<Tuple> result = userDao.companyNameBirthDayUsers(session);
        List<String> companyName = result.stream().map(r -> (r.get(0, Company.class)).getName()).toList();
        assertThat(companyName).hasSize(5);
        assertThat(companyName).contains("Apple", "Apple", "Google", "Google", "Microsoft");

        List<LocalDate> birthdayList = result.stream().map(r -> (r.get(1, Birthday.class)).birthDate()).toList();
        birthdayList.forEach(System.out::println);
        assertThat(birthdayList).hasSize(5);
        session.getTransaction().commit();
    }

    @Test
    void countUsersOnTheCompany() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();
        Long countUsersInOneCompany = userDao.countUsersInOneCompany(session, "Google");
        assertNotNull(countUsersInOneCompany);
        assertEquals(countUsersInOneCompany,2);
        session.getTransaction().commit();
    }
}
