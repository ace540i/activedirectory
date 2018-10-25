import java.util.Hashtable;
import java.util.jar.Attributes;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;


class TestAD 
{ 
  static DirContext ldapContext; 
  public static void main2 (String[] args) throws NamingException 
  { 
    try 
    { 
      System.out.println("Début du test Active Directory"); 

      Hashtable<String, String> ldapEnv = new Hashtable<String, String>(11); 
      ldapEnv.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory"); 
      //ldapEnv.put(Context.PROVIDER_URL,  "ldap://societe.fr:389"); 
      ldapEnv.put(Context.PROVIDER_URL,  "ldap://spectraestnj.com:389"); 
      ldapEnv.put(Context.SECURITY_AUTHENTICATION, "simple"); 
      //ldapEnv.put(Context.SECURITY_PRINCIPAL, "cn=administrateur,cn=users,dc=societe,dc=fr"); 
      ldapEnv.put(Context.SECURITY_PRINCIPAL, "cn=mdarretta,ou=MonOu,dc=dom,dc=con"); 
      ldapEnv.put(Context.SECURITY_CREDENTIALS, "Poiqwe124"); 
      //ldapEnv.put(Context.SECURITY_PROTOCOL, "ssl"); 
      //ldapEnv.put(Context.SECURITY_PROTOCOL, "simple"); 
      ldapContext = new InitialDirContext(ldapEnv); 

      // Create the search controls          
      SearchControls searchCtls = new SearchControls(); 

      //Specify the attributes to return 
      String returnedAtts[]={"sn","givenName", "samAccountName"}; 
      searchCtls.setReturningAttributes(returnedAtts); 

      //Specify the search scope 
      searchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE); 

      //specify the LDAP search filter 
      String searchFilter = "(&(objectClass=user))"; 

      //Specify the Base for the search 
      String searchBase = "dc=dom,dc=fr"; 
      //initialize counter to total the results 
      int totalResults = 0; 

      // Search for objects using the filter 
      NamingEnumeration<SearchResult> answer = ldapContext.search(searchBase, searchFilter, searchCtls); 

      //Loop through the search results 
      while (answer.hasMoreElements()) 
      { 
        SearchResult sr = (SearchResult)answer.next(); 

        totalResults++; 

        System.out.println(">>>" + sr.getName()); 
        Attributes attrs = (Attributes) sr.getAttributes(); 
        System.out.println(">>>>>>" + attrs.get("samAccountName")); 
      } 

      System.out.println("Total results: " + totalResults); 
      ldapContext.close(); 
    } 
    catch (Exception e) 
    { 
      System.out.println(" Search error: " + e); 
      e.printStackTrace(); 
      System.exit(-1); 
    } 
  } 
}