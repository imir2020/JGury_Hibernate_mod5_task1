package by.itacademy.hibernate.dao;


import antlr.DefaultJavaCodeGeneratorPrintWriterManager;
import by.itacademy.hibernate.entity.Payment;
import by.itacademy.hibernate.entity.QUser;
import by.itacademy.hibernate.entity.User;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.Order;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.QBean;
import com.querydsl.jpa.impl.JPAQuery;
import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.NoArgsConstructor;
import org.hibernate.Session;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static by.itacademy.hibernate.entity.QCompany.company;
import static by.itacademy.hibernate.entity.QPayment.payment;
import static by.itacademy.hibernate.entity.QUser.user;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class UserDao {

    private static final UserDao INSTANCE = new UserDao();

    /**
     * Возвращает всех сотрудников
     */
    public List<User> findAll(Session session) {
        return new JPAQuery<User>(session)
                .select(user)
                .from(user)
                .fetch();
    }

    /**
     * Возвращает всех сотрудников с указанным именем
     */
    public List<User> findAllByFirstName(Session session, String firstName) {
        return new JPAQuery<User>(session)
                .select(user)
                .from(user)
                .where(user.personalInfo().firstname.eq(firstName))
                .fetch();

    }

    /**
     * Возвращает первые {limit} сотрудников, упорядоченных по дате рождения (в порядке возрастания)
     */
    public List<User> findLimitedUsersOrderedByBirthday(Session session, int limit) {
        return new JPAQuery<User>(session)
                .from(user)
                .orderBy(new OrderSpecifier(Order.ASC, user.personalInfo().birthDate))
                .limit(limit)
                .fetch();
    }

    /**
     * Возвращает всех сотрудников компании с указанным названием
     */
    public List<User> findAllByCompanyName(Session session, String companyName) {
        return new JPAQuery<User>(session)
                .select(user)
                .from(company)
                .join(company.users, user)
                .where(company.name.eq(companyName))
                .fetch();
    }

    /**
     * Возвращает все выплаты, полученные сотрудниками компании с указанными именем,
     * упорядоченные по имени сотрудника, а затем по размеру выплаты
     */
    public List<Payment> findAllPaymentsByCompanyName(Session session, String companyName) {
        return new JPAQuery<Payment>(session)
                .select(payment)
                .from(company)
                .join(company.users, user)
                .join(user.payments, payment)
                .where(company.name.eq(companyName))
                .orderBy(user.personalInfo().firstname.asc(), payment.amount.asc())
                .fetch();

    }

    /**
     * Возвращает среднюю зарплату сотрудника с указанными именем и фамилией
     */
    public Double findAveragePaymentAmountByFirstAndLastNames(Session session, String firstName, String lastName) {
        return new JPAQuery<Double>(session)
                .select(payment.amount.avg())
                .from(payment)
                .join(payment.receiver(), user)
                .where(user.personalInfo().firstname.eq(firstName).and(user.personalInfo().lastname.eq(lastName)))
                .fetchOne();
    }

    /**
     * Возвращает для каждой компании: название, среднюю зарплату всех её сотрудников. Компании упорядочены по названию.
     */
    public List<Tuple> findCompanyNamesWithAvgUserPaymentsOrderedByCompanyName(Session session) {
        return new JPAQuery<Tuple>(session)
                .select(company.name, payment.amount.avg())
                .from(company)
                .join(company.users, user)
                .join(user.payments, payment)
                .groupBy(company.name)
                .orderBy(company.name.asc())
                .fetch();
    }

    /**
     * Возвращает список: сотрудник (объект User), средний размер выплат,
     * но только для тех сотрудников, чей средний размер выплат
     * больше среднего размера выплат всех сотрудников
     * Упорядочить по имени сотрудника
     */
    public List<Tuple> isItPossible(Session session) {
        return new JPAQuery<Tuple>(session)
                .select(user, payment.amount.avg())
                .from(user)
                .join(user.payments, payment)
                .groupBy(user.id)
                .having(payment.amount.avg().gt(
                        new JPAQuery<Double>(session)
                                .select(payment.amount.avg())
                                .from(payment)
                ))
                .orderBy(user.personalInfo().firstname.asc())
                .fetch();
    }
    /**
     * Возвращает всех сотрудников с их средней зарплатой.
     */
    public List<Tuple> usersAvgPayments(Session session) {
        return new JPAQuery<Tuple>(session)
                .select(user, payment.amount.avg())
                .from(payment)
                .join(payment.receiver(),user)
                .groupBy(user.id)
                .fetch();
    }

    /**
     * Возвращает список: сотрудник (объект User), средний размер выплат,
     * но только для тех сотрудников, чей средний размер выплат
     * меньше или равен среднему размеру выплат всех сотрудников
     * Упорядочить по фамилии сотрудника
     */
    public List<Tuple> paymentsLessUserAvgPayment(Session session) {
        return new JPAQuery<Tuple>(session)
                .select(user, payment.amount.avg())
                .from(user)
                .join(user.payments, payment)
                .groupBy(user.id)
                .where(payment.amount.lt(
                        new JPAQuery<Double>(session)
                                .select(payment.amount.avg())
                                .from(payment)
                ))
                .orderBy(user.personalInfo().lastname.desc(),payment.amount.avg().desc())
                .fetch();
    }

    /**
     * Возвращает всех сотрудников с указанной фамилией и выплатами
     */
    public List<Payment> userWithLastNameAndPayments(Session session, String lastname) {
        return new JPAQuery<Payment>(session)
                .select(payment)
                .from(payment)
                .join(payment.receiver(),user)
                .where(user.personalInfo().lastname.eq(lastname))
                .fetch();
    }

    /**
     * Возвращает всех сотрудников, упорядоченных по среднему размеру выплат
     * (в порядке УБЫВАНИЯ)
     */
    public List<User> usersOrderedByAvgPayments(Session session) {
        return new JPAQuery<User>(session)
                .select(user)
                .from(user)
                .leftJoin(user.payments, payment)
                .groupBy(user)
                .orderBy(payment.amount.avg().desc())
                .fetch();
    }

    /**
     * Возвращает для каждой компании: название, возраст всех её сотрудников.
     * Компании и возраст сотрудников упорядочены(компании упорядочены по названию).
     */
    public List<Tuple> companyNameBirthDayUsers(Session session) {
        return new JPAQuery<Tuple>(session)
                .select(company, user.personalInfo().birthDate)
                .from(user)
                .join(user.company(), company)
                .groupBy(company.id, user.personalInfo().birthDate)
                .orderBy(company.name.asc(), new OrderSpecifier(Order.ASC,user.personalInfo().birthDate))
                .fetch();
    }


    /**
     количество сотрудников в заданной компании.
     */
    public Long countUsersInOneCompany(Session session, String companyName) {
        return new JPAQuery<Long>(session)
                .select(user.count())
                .from(user)
                .join(user.company(), company)
                .groupBy(company.name)
                .where(company.name.eq(companyName))
                .orderBy(company.name.asc())
                .fetchOne();
    }


    public static UserDao getInstance() {
        return INSTANCE;
    }
}