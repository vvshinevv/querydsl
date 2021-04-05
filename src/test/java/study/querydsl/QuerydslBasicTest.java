package study.querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    private EntityManager em;

    private JPAQueryFactory queryFactory;

    @BeforeEach
    public void before() {
        queryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL() {
        // member1 을 찾아라

        Member findMember = em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }


    @Test
    public void startQuerydsl() {
        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assert findMember != null;
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() {
        Member findMember = queryFactory.
                selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();

        assert findMember != null;
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam() {
        Member findMember = queryFactory.
                selectFrom(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10)
                )
                .fetchOne();

        assert findMember != null;
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() {
        List<Member> fetch = queryFactory
                .selectFrom(member)
                .fetch();

        Member fetchOne = queryFactory
                .selectFrom(member)
                .fetchOne();

        Member fetchFirst = queryFactory
                .selectFrom(member)
                .fetchFirst();

        QueryResults<Member> fetchResults = queryFactory
                .selectFrom(member)
                .fetchResults();

        long total = fetchResults.getTotal();
        List<Member> content = fetchResults.getResults();
    }

    /**
     * 회원 정렬
     * 1. 회원 나이 내림차순
     * 2. 회원 이름 올림차순
     * <p>
     * 단, 2에서 회원 이름이 없으면 마지막에 출
     */
    @Test
    public void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();


        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);


        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();
    }

    @Test
    public void paging1() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1).limit(2).fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void paging2() {
        QueryResults<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1).limit(2).fetchResults();

        assertThat(result.getTotal()).isEqualTo(4);
        assertThat(result.getLimit()).isEqualTo(2);
        assertThat(result.getOffset()).isEqualTo(1);
        assertThat(result.getResults().size()).isEqualTo(2);
    }

    @Test
    public void aggregation() {
        List<Tuple> fetch = queryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();


        Tuple tuple = fetch.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);
    }

    /**
     * 팀 이름과 각 팀의 평균 연령을 구하라.
     */
    @Test
    public void groupBy() {
        List<Tuple> fetch = queryFactory.select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = fetch.get(0);
        Tuple teamB = fetch.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35);
    }

    /**
     * 팀 A에 소속된 모든 회원을 구하라.
     */
    @Test
    public void join() {
        List<Member> fetch = queryFactory
                .select(member)
                .from(member)
                .join(member.team, team)
                .where(member.team.name.eq("teamA"))
                .fetch();

        assertThat(fetch).extracting("username").containsExactly("member1", "member2");
    }

    /**
     * 세타 조인
     * 회원 이름이 팀 이름과 같은 회원 조회
     */
    @Test
    public void theta_join() {

        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));


        List<Member> fetch = queryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        assertThat(fetch).extracting("username").containsExactly("teamA", "teamB");
    }

    /**
     * 회원과 팀을 조인하면서 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     * JPQL : select m, t from Member m left join m.team t on t.name = 'teamA';
     */
    @Test
    public void join_on_filtering() {
        List<Tuple> result = queryFactory.select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .on(member.team.name.eq("teamA"))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple : " + tuple);
        }

    }


    @PersistenceUnit
    private EntityManagerFactory emf;

    @Test
    public void fetchJoinNo() {
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assert findMember != null;
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 미적용").isFalse();
    }

    @Test
    public void fetchJoinUse() {
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        assert findMember != null;
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 적용").isTrue();
    }

    /**
     * 나이가 가장 많은 회원을 조회
     */
    @Test
    public void subQuery() {

        QMember memberSub = new QMember("memberSub");
        List<Member> result = queryFactory
                .selectFrom(member)
                .where(
                        member.age.eq(
                                JPAExpressions
                                        .select(memberSub.age.max())
                                        .from(memberSub)
                        )
                )
                .fetch();

        assertThat(result.get(0).getAge()).isEqualTo(40);

    }

    /**
     * 나이가 가장 평균 이상인 회원을 조회
     */
    @Test
    public void subQueryGoe() {

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(
                        member.age.goe(
                                JPAExpressions
                                        .select(memberSub.age.avg())
                                        .from(memberSub)
                        )
                )
                .fetch();

        assertThat(result).extracting("age").containsExactly(30, 40);
    }

    /**
     * 나이가 가장 평균 이상인 회원을 조회
     */
    @Test
    public void subQueryIn() {

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(
                        member.age.in(
                                JPAExpressions
                                        .select(memberSub.age)
                                        .from(memberSub)
                                        .where(memberSub.age.gt(10))
                        )
                )
                .fetch();

        assertThat(result).extracting("age").containsExactly(20, 30, 40);
    }

    @Test
    public void selectSubQuery() {
        QMember memberSub = new QMember("memberSub");
        List<Tuple> result = queryFactory
                .select(member.username,
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                )
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void basicCase() {
        List<String> result = queryFactory
                .select(
                        member.age.when(10).then("열살").when(20).then("스무살").otherwise("기타")
                )
                .from(member)
                .fetch();

        for (String age : result) {
            System.out.println("age -> " + age);
        }
    }

    @Test
    public void complexCate() {
        List<String> result = queryFactory
                .select(
                        new CaseBuilder()
                                .when(member.age.between(0, 20)).then("0~20살")
                                .when(member.age.between(21, 30)).then("21~30살")
                                .otherwise("기타"))
                .from(member)
                .fetch();

        for (String age : result) {
            System.out.println("age -> " + age);
        }
    }

    @Test
    public void constant() {
        List<Tuple> result = queryFactory
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();

        for(Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void concat() {
        // {username}_{age}
        List<String> result = queryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("member1"))
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }
}
