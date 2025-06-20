package datawave.webservice.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;

public class TestDatawaveUserProperties {

    private String subjectDn;
    private String issuerDn;
    private String email;
    private String userType;
    private Collection<String> auths;
    private Collection<String> roles;
    private Multimap<String,String> roleToAuthMapping;

    public TestDatawaveUserProperties() {
        System.out.println("TestDatawaveUserProperties constructor called");
    }

    public String getSubjectDn() {
        return subjectDn;
    }

    public void setSubjectDn(String subjectDn) {
        this.subjectDn = subjectDn;
    }

    public String getIssuerDn() {
        return issuerDn;
    }

    public void setIssuerDn(String issuerDn) {
        this.issuerDn = issuerDn;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getUserType() {
        return userType;
    }

    public void setUserType(String userType) {
        this.userType = userType;
    }

    public Collection<String> getAuths() {
        return auths;
    }

    public void setAuths(Collection<String> auths) {
        this.auths = auths;
    }

    public Collection<String> getRoles() {
        return roles;
    }

    public void setRoles(Collection<String> roles) {
        this.roles = roles;
    }

    public Multimap<String,String> getRoleToAuthMapping() {
        return roleToAuthMapping;
    }

    public void setRoleToAuthMapping(Map<String,String> roleToAuthMapping) {
        Multimap<String,String> multiMap = HashMultimap.create();
        roleToAuthMapping.forEach((key, value) -> multiMap.putAll(key, Arrays.stream(value.split(",")).map(String::trim).collect(Collectors.toList())));
        this.roleToAuthMapping = multiMap;
    }

    public DatawaveUser toDatawaveUser() {
        SubjectIssuerDNPair dnPair = toSubjectIssuerDNPair();
        DatawaveUser.UserType userType = DatawaveUser.UserType.valueOf(this.userType);
        long creationTime = System.currentTimeMillis();
        long expirationTime = creationTime + 24 * 60 * 60 * 1000; // add 24 hours to creationTime
        return new DatawaveUser(dnPair, userType, this.email, this.auths, this.roles, this.roleToAuthMapping, creationTime, expirationTime);
    }

    public SubjectIssuerDNPair toSubjectIssuerDNPair() {
        return SubjectIssuerDNPair.of(this.subjectDn, this.issuerDn);
    }
}
