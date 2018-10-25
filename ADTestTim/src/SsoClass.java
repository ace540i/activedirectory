
/** *********************************************************
 * Process SSO (Single Sign On, Active Directoty / LDAP)
 * 
 * @author tcondron
 * @version Aug 1, 2015
 * 
 * **********************************************************
 */

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


public class SsoClass  {

	boolean debug = true; //false;

	String app;
	
	Attributes attrs = null;

	public SsoClass () {

		
		if (this.debug) System.out.println("- SsoClass() - Constructor... ");
	}
	
	public SsoClass (String app) {
		this.app = app;
		 
		if (this.debug) System.out.println("- SsoClass() - Constructor... For: " + app);
	}
	

            
	public Attributes get_attributes(String username)
    {
            final String ATTRIBUTE_FOR_USER = "sAMAccountName";
            final String _domain  = "spectraeastnj.com";
            final String host 	  = "spectraeastnj.com";
            final String dn		  = "DC=spectraeastnj,DC=com";

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
                            	  System.out.println("attributes "+attrs.get(ATTRIBUTE_FOR_USER));
                                    return attrs;
                              }
                        }
                   }
                  catch (NamingException e)
                  {
                        System.out.println("#####################################################");
                        System.out.println("### ERROR, Unable to get <InitialLdapContext>.... ###");
                        System.out.println("#####################################################");
                        //e.printStackTrace();
                  }
                  return null;
    }
            
        public String parse_attributes()
        {
        	String cn = "";
        	
      	    //String returnedAtts[] ={ "cn", "name", "mail", "sAMAccountName", "distingusishedName" };
          	
        	if (this.debug) System.out.println(this.attrs.get("cn")); //String s1 = att.get("cn").toString();
			cn = this.attrs.get("cn").toString();
			cn = cn.substring(cn.indexOf(":")+2);
			if (this.debug) System.err.println("cn=" + cn);
			
			if (this.debug) System.out.println(this.attrs.get("sAMAccountName")); //String s2 = att.get("sAMAccountName").toString();
			if (this.debug) System.out.println(this.attrs.get("name")); //String s3 = att.get("name").toString();
			if (this.debug) System.out.println(this.attrs.get("distinguishedName")); //String s4 = att.get("distinguishedName").toString();
			if (this.debug) {
				if (this.attrs.get("mail") != null) {
					System.out.println(this.attrs.get("mail")); //String s5 = att.get("mail").toString();
					System.out.println(this.attrs.get("telephonenumber"));
				}            	
			}
        	
        	return cn;
        }

        public int get_memebers(String cnToMatch, String sooApp) {
        //  public String get_memebers(String cnToMatch, String sooApp) {
        	 
        	//String msg="Sorry no matching entry for: "+cnToMatch+ " in the SSO-" + sooApp;
        	int msg = 0;
        	
        	Attributes attrs;

        	if (this.debug) System.out.println("Searching GetMemebers for: CN="+cnToMatch + " for the SSO: " + sooApp);

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
     	     //   System.out.println("attribute "+ attr.get());
     	         String id = attr.getID();
                  
     	         NamingEnumeration<?> valueEnum = attr.getAll(); 
     	         while (valueEnum.hasMore()) 
     	         {
     	            Object value = valueEnum.next();
     	            //System.out.println(id+ ") value: "+ value);
     	            
     	           if ( id.equals("name") || id.equals("member") ) {
     	            	System.out.println(id+ "-> "+ value);
     	 			   	String cn[] = value.toString().split(","); 
     	 			  if (this.debug) System.out.println("GetMemebers: "+ id+ "-> "+ cn[0] + "");

     	 	           if ( cn[0].equals("CN="+cnToMatch) ) {
     	 	        	 msg = 1;
     	 	        	 //msg="Matching entry found for: "+cnToMatch+ " in the SSO: " + sooApp;
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


	
