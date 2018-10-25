
// http://blogs.artinsoft.net/Mrojas/archive/2007/01/10/Java-and-Active-Directory.aspx
	
//import java.io.BufferedReader;
//import java.io.InputStreamReader;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

public class SSO_Reference
{
    public static void main(String[] args) throws Exception
    {
  	  	String[] ssoApp = {"PENDING", "BUFFER", "PLAC", "SOMETHINGELSE"};
  	  	String username = "";
  	  	String app = "";
  	  	String cn = "";
  	  
        String[] userid = {"joeblow", "tcondron", "rshur", "rodriguezr", "rosullivan", "sikudithipudi"};
        //System.err.println("userid=" + userid[0]);
        
			//    InputStreamReader converter = new InputStreamReader(System.in);
			//    BufferedReader in = new BufferedReader(converter);
			//    System.out.println("Please type username:");
			//    String username = in.readLine();
			//	    	System.out.println("Please type password:");
			//	    	String password = in.readLine();

          username = userid[3];
          System.err.println("userid=" + username);
          //	System.out.println(System.getProperty("user.name"));
          //	username = System.getProperty("user.name");
          


/* * * * * * ** * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *  
*  First Step using USERID(network-id): 
*  	Lookup in AD (sAMAccountName=USERID) and get the 'CN' (Common Name) given for match..
*  	Then keep the CN to do lookup in SSO-application, as the sAMAccountName is not available...!!!              
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *     
*/
          GetActiveDir ad = new GetActiveDir();
          Attributes att = ad.get_attributes(username); //att = ldap.authenticateUser(username, "spectraeastnj.com", "spectraeastnj.com", "DC=spectraeastnj,DC=com");
          
          if (att == null)
          {
                System.out.println("Sorry your User-id (network-id) is invalid...");
          }
          else
          {
        	  
        	  cn = ad.parse_attributes();
	          //ga = null;
	          
	          app = ssoApp[0];
	          System.err.println("App=" + app);
	          
	          String msg = ad.get_memebers(cn, app);
	          System.out.println("-----------------------------------------------------------------------"); 
	          System.out.println("-- " + msg.toUpperCase()); 
	          System.out.println("-----------------------------------------------------------------------"); 

          }
          ad = null;          
          
    } //main
    

    static class GetActiveDir    
    {
        static String ATTRIBUTE_FOR_USER = "sAMAccountName";
        
        private Attributes attrs = null;
            
        public Attributes get_attributes(String username) //, String _domain, String host, String dn)
        {
				String _domain 	= "spectraeastnj.com";
				String host 	= "spectraeastnj.com";
				String dn		= "DC=spectraeastnj,DC=com";
            	
                  String returnedAtts[] ={ "cn", "name", "mail", "sAMAccountName", "distinguishedName","telephonenumber" };
                  
                  String searchFilter = "(&(objectClass=user)(" + ATTRIBUTE_FOR_USER + "=" + username + "))";
                  //Create the search controls
                  SearchControls searchCtls = new SearchControls();
                  searchCtls.setReturningAttributes(returnedAtts);
                  //Specify the search scope
                  searchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE);
                  String searchBase = dn;
                  Hashtable environment = new Hashtable();
                  environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
                  environment.put(Context.PROVIDER_URL, "ldap://" + host + ":389");
                  environment.put(Context.SECURITY_AUTHENTICATION, "simple");
                  environment.put(Context.SECURITY_PRINCIPAL, "ssoldaptest" + "@" + _domain);
                  environment.put(Context.SECURITY_CREDENTIALS,"Likely!2");
                  LdapContext ctxGC = null;
                  try
                  {
                        ctxGC = new InitialLdapContext(environment, null);
                        //    Search for objects in the GC using the filter

                        NamingEnumeration answer = ctxGC.search(searchBase, searchFilter, searchCtls);
                        while (answer.hasMoreElements())
                        {
                              SearchResult sr = (SearchResult)answer.next();
                              this.attrs = sr.getAttributes();
                              if (attrs != null)
                              {
                                    return attrs;
                              }
                        }
                   }
                  catch (NamingException e)
                  {
                        System.out.println("Just reporting error");
                        e.printStackTrace();
                  }
                  return null;
         }
            
        public String parse_attributes()
        {
        	String cn = "";
        	
      	  //String returnedAtts[] ={ "cn", "name", "mail", "sAMAccountName", "distingusishedName" };
          	
			System.out.println(this.attrs.get("cn")); //String s1 = att.get("cn").toString();
			cn = this.attrs.get("cn").toString();
			cn = cn.substring(cn.indexOf(":")+2);
			System.err.println("cn=" + cn);
			
			System.out.println(this.attrs.get("sAMAccountName")); //String s2 = att.get("sAMAccountName").toString();
			System.out.println(this.attrs.get("name")); //String s3 = att.get("name").toString();
			System.out.println(this.attrs.get("distinguishedName")); //String s4 = att.get("distinguishedName").toString();
			  
			if (this.attrs.get("mail") != null) {
				System.out.println(this.attrs.get("mail")); //String s5 = att.get("mail").toString();
				System.out.println("telephone "+this.attrs.get("telephonenumber"));
			}            	
        	
        	return cn;
        }

        String get_memebers(String cnToMatch, String sooApp) {
        	 
        	String msg="Sorry no matching entry for: "+cnToMatch+ " in the SSO-" + sooApp;
        	Attributes attrs;

        	System.out.println("Searching GetMemebers for: CN="+cnToMatch + " for the SSO: " + sooApp);

     	   try
     	   {                   
     		  Hashtable<String, String> env = new Hashtable<String, String>();
     		  env.put(Context.SECURITY_PRINCIPAL, "ssoldaptest" + "@spectraeastnj.com");
     		  env.put(Context.SECURITY_CREDENTIALS, "Likely!2");
     		  DirContext initial = new InitialDirContext(env);
     		  DirContext context = (DirContext) initial.lookup("ldap://spectraeastnj.com:389");
     		   
     		  String dn = "CN="+sooApp+",OU=AppsAccessTestSSO,OU=MIS Department,DC=spectraeastnj,DC=com";
     		  //->String dn = "CN=PENDING,OU=AppsAccessTestSSO,OU=MIS Department,DC=spectraeastnj,DC=com";
     		  //  String dn = "CN=Tim Condron,OU=Patch Test,OU=Users,OU=MIS Department,dc=spectraeastnj,dc=com";
     		  
     		  attrs = context.getAttributes(dn);

     	      NamingEnumeration<? extends Attribute> attrEnum = attrs.getAll(); 
     	      while (attrEnum.hasMore()) 
     	      {
     	         Attribute attr = attrEnum.next();
     	         String id = attr.getID();

     	         NamingEnumeration<?> valueEnum = attr.getAll(); 
     	         while (valueEnum.hasMore()) 
     	         {
     	            Object value = valueEnum.next();
     	            //System.out.println(id+ ") value: "+ value);
     	            
     	           if ( id.equals("name") || id.equals("member") ) {
     	            	//System.out.println(id+ "-> "+ value);
     	 			   	String cn[] = value.toString().split(","); 
     	 			   	System.out.println("GetMemebers: "+ id+ "-> "+ cn[0] + "");

     	 	           if ( cn[0].equals("CN="+cnToMatch) ) {
     	 	        	 msg="Matching entry found for: "+cnToMatch+ " in the SSO: " + sooApp;
     	 	           }
     	 	           
     	           }

     	           
     	            	
     	            if (id.equals("userPassword"))
     	               value = new String((byte[]) value);            


     	            if (!id.equals("uid"))
     	            {
     					//   System.out.println("idLabel: "+ idLabel);
     					//   System.out.println("valueField: "+ valueField);
     	            }
     	         }
     	      }	      
     	      
     	   }
     	   catch (NamingException e) 
     	   {
     	      //JOptionPane.showMessageDialog(this, e);
     	   }    
     	   
     	   return msg;
        	
    	} // getMemebers        
        
        
    } //GetAttributes


	//	  A few naming abbreviations:
	//		  cn 	Common Name
	//		  ou 	Organizational Unit
	//		  dc 	Domain Component
	//		  dn 	Distinguished Name
	//		  RDN 	Relative Distinguished Name
	//		  UPN 	User Principal Name
/*
 * 
userid=tcondron
App=PENDING
cn: Tim Condron
sAMAccountName: tcondron
name: Tim Condron
distinguishedName: CN=Tim Condron,OU=Patch Test,OU=Users,OU=MIS Department,DC=spectraeastnj,DC=com
mail: Tim.Condron@fmc-na.com
Searching GetMemebers for: CN=Tim Condron for the SSO: PENDING
GetMemebers: name-> PENDING
GetMemebers: member-> CN=S-1-5-21-1851324978-1562931139-1507752641-7147
GetMemebers: member-> CN=NJSiva Kudithipudi
GetMemebers: member-> CN=NJRod Rodriguez
GetMemebers: member-> CN=NJReg O'Sullivan
GetMemebers: member-> CN=Tim Condron
-----------------------------------------------------------------------
-- MATCHING ENTRY FOUND FOR: TIM CONDRON IN THE SSO: PENDING
-----------------------------------------------------------------------
--------------------------------------------
userid=rshur
cn: Rina Shur
sAMAccountName: rshur
name: Rina Shur
distinguishedName: CN=Rina Shur,OU=User,OU=Disabled Objects,DC=spectraeastnj,DC=com
Searching GetMemebers for: CN=Rina Shur for the SSO: PENDING
App=PENDING
GetMemebers: name-> PENDING
GetMemebers: member-> CN=S-1-5-21-1851324978-1562931139-1507752641-7147
GetMemebers: member-> CN=NJSiva Kudithipudi
GetMemebers: member-> CN=NJRod Rodriguez
GetMemebers: member-> CN=NJReg O'Sullivan
GetMemebers: member-> CN=Tim Condron
-----------------------------------------------------------------------
-- SORRY NO MATCHING ENTRY FOR: RINA SHUR IN THE SSO-PENDING
-----------------------------------------------------------------------
*/

}
	
