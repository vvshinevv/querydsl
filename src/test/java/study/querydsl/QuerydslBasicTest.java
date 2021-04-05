package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
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

    @Test
    public void simpleProjection() {
        List<String> results = queryFactory
                .select(member.username)
                .from(member)
                .fetch();

        for (String result : results) {
            System.out.println("result => " + result);
        }
    }

    @Test
    public void tupleProjection() {
        List<Tuple> tuples = queryFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();

        for (Tuple tuple : tuples) {
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);
            System.out.println(username);
            System.out.println(age);
        }
    }

    @Test
    public void findDtoByJPQL() {
        List<MemberDto> results =
                em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age) from Member m", MemberDto.class)
                        .getResultList();

        for (MemberDto result : results) {
            System.out.println("memberDto = " + result);
        }
    }

    @Test
    public void findDtoBySetter() {
        List<MemberDto> results = queryFactory
                .select(Projections.bean(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto result : results) {
            System.out.println("memberDto = " + result);
        }
    }

    @Test
    public void findDtoByField() {
        List<MemberDto> results = queryFactory
                .select(Projections.fields(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto result : results) {
            System.out.println("memberDto = " + result);
        }
    }

    @Test
    public void findDtoByConstructor() {
        List<MemberDto> results = queryFactory
                .select(Projections.constructor(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto result : results) {
            System.out.println("memberDto = " + result);
        }
    }

    @Test
    public void findUserDto() {
        List<UserDto> results = queryFactory
                .select(Projections.fields(UserDto.class, member.username.as("name"), member.age))
                .from(member)
                .fetch();

        for (UserDto result : results) {
            System.out.println("UserDto = " + result);
        }
    }

    @Test
    public void findUserDtoSubQuery() {
        QMember memberSub = new QMember("memberSub");
        List<UserDto> results = queryFactory
                .select(Projections.fields(
                        UserDto.class,
                        member.username.as("name"),
                        ExpressionUtils.as(
                                JPAExpressions.select(
                                        memberSub.age.max()
                                ).from(memberSub), "age"
                        )
                        )
                )
                .from(member)
                .fetch();

        for (UserDto result : results) {
            System.out.println("UserDto = " + result);
        }
    }

    /**
     * 런타임 오류가 난다.
     */
    @Test
    public void findUserDtoByConstructor() {
        List<UserDto> results = queryFactory
                .select(Projections.constructor(UserDto.class, member.username, member.age))
                .from(member)
                .fetch();

        for (UserDto result : results) {
            System.out.println("UserDto = " + result);
        }
    }

    @Test
    public void findDtoByQueryProjection() {
        List<MemberDto> results = queryFactory
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();

        for (MemberDto result : results) {
            System.out.println("memberDto = " + result);
        }
    }

    @Test
    public void dynamicQuery_booleanBuilder() {
        String username = "member1";
        Integer ageParam = null;

        List<Member> result = searchMember1(username, ageParam);
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember1(String username, Integer age) {

        BooleanBuilder builder = new BooleanBuilder();
        if (username != null) {
            builder.and(member.username.eq(username));
        }

        if (age != null) {
            builder.and(member.age.eq(age));
        }

        return queryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();
    }

    @Test
    public void dynamicQuery_whereParam() {
        String username = "member1";
        Integer ageParam = 10;

        List<Member> result = searchMember2(username, ageParam);
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember2(String username, Integer age) {
        return queryFactory
                .selectFrom(member)
                .where(usernameEq(username), ageEq(age))
                .fetch();
    }

    private BooleanExpression ageEq(Integer age) {
        return age != null ? member.age.eq(age) : null;
    }

    private BooleanExpression usernameEq(String username) {
        return username != null ? member.username.eq(username) : null;
    }

    private BooleanExpression allEq(String username, Integer age) {
        return ageEq(age).and(usernameEq(username));
    }

    @Test
    public void bulkUpdate() {


        // member1 = 10 -> DB member1
        // member2 = 20 -> DB member2
        // member3 = 30 -> DB member3
        // member4 = 40 -> DB member4

        long count = queryFactory
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();

        assertThat(count).isEqualTo(2);

        em.flush();
        em.clear();

        // member1 = 10 -> DB 비회원
        // member2 = 20 -> DB 비회원
        // member3 = 30 -> DB member3
        // member4 = 40 -> DB member4

        List<Member> results = queryFactory
                .selectFrom(member)
                .fetch();

        for (Member result : results) {
            System.out.println("result -> " + result);
        }
    }

    @Test
    public void bulkAdd() {
        long count = queryFactory
                .update(member)
                .set(member.age, member.age.add(1))
                .execute();

        assertThat(count).isEqualTo(4);

        List<Member> results = queryFactory
                .selectFrom(member)
                .fetch();

        // 영속성 컨텍스트에 있는 값을 가져오기 때문에 DB 벌크 업데이트를 칠 때 주의해야한다.
        for (Member result : results) {
            System.out.println("result -> " + result);
        }
    }

    @Test
    public void bulkMultiply() {
        long count = queryFactory
                .update(member)
                .set(member.age, member.age.multiply(2))
                .execute();
    }

    @Test
    public void bulkDelete() {
        long count = queryFactory
                .delete(member)
                .where(member.age.gt(18))
                .execute();
    }

    @Test
    public void sqlFunction1() {
        List<String> results = queryFactory
                .select(Expressions.stringTemplate(
                        "function('replace', {0}, {1}, {2})",
                        member.username, "member", "M"
                ))
                .from(member)
                .fetch();

        for (String result : results) {
            System.out.println("result -> " + result);
        }
    }

    @Test
    public void sqlFunction2() {
        queryFactory
                .select(member.username)
                .from(member)
                //.where(member.username.eq(Expressions.stringTemplate("function('lower', {0})", member.username)))
                .where(member.username.eq(member.username.lower()))
                .fetch();
    }
}
