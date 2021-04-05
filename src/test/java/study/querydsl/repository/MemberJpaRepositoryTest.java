package study.querydsl.repository;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberSearchCondition;
import study.querydsl.dto.MemberTeamDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
class MemberJpaRepositoryTest {

    @Autowired
    private EntityManager em;

    @Autowired
    private MemberJpaRepository memberJpaRepository;

    @Test
    public void basicTest() {
        Member member1 = new Member("username1", 10);
        memberJpaRepository.save(member1);

        Member findByIdMember = memberJpaRepository.findById(member1.getId()).get();
        assertThat(findByIdMember).isEqualTo(member1);

        List<Member> members = memberJpaRepository.findAll();
        assertThat(members).containsExactly(member1);

        List<Member> findByUsernameMember = memberJpaRepository.findByUserName("username1");
        assertThat(findByUsernameMember).containsExactly(member1);
    }

    @Test
    public void basicQuerydslTest() {
        Member member1 = new Member("username1", 10);
        memberJpaRepository.save(member1);

        List<Member> members = memberJpaRepository.findAllQuerydsl();
        assertThat(members).containsExactly(member1);

        List<Member> findByUsernameMember = memberJpaRepository.findByUserNameQuerydsl("username1");
        assertThat(findByUsernameMember).containsExactly(member1);
    }

    @Test
    public void searchTest() {
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

        MemberSearchCondition condition = new MemberSearchCondition();
        condition.setAgeGoe(35);
        condition.setAgeLoe(40);
        condition.setTeamName("teamB");

        List<MemberTeamDto> result = memberJpaRepository.search(condition);
        assertThat(result).extracting("username").containsExactly("member4");
    }

}
