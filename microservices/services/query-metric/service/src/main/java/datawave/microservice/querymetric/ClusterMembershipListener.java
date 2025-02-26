package datawave.microservice.querymetric;

import java.util.Set;

import org.apache.log4j.Logger;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;

public class ClusterMembershipListener implements MembershipListener {
    
    private Logger log = Logger.getLogger(ClusterMembershipListener.class);
    
    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        Set<Member> members = membershipEvent.getCluster().getMembers();
        log.info("added member: " + membershipEvent.getMember().getUuid() + ":" + membershipEvent.getMember().getAddress().toString() + " cluster now ("
                        + members.size() + ") " + members);
    }
    
    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        Set<Member> members = membershipEvent.getCluster().getMembers();
        log.info("removed member: " + membershipEvent.getMember().getUuid() + ":" + membershipEvent.getMember().getAddress().toString() + " cluster now ("
                        + members.size() + ") " + members);
    }
}
