import javax.naming.directory.Attributes;

public class SSO_TEST {
   	
    public static void main(String[] args) throws Exception
    { 
  	  	String username = "mdarretta";
  	  	String cn = "";
          SsoClass sso = new SsoClass();			       
          Attributes att = sso.get_attributes(username);         
          if (att == null)
          {
                System.out.println("Sorry your User-id (network-id) is invalid...");
          }
          else
          {			
       	  cn = sso.parse_attributes();
        	  System.out.println(cn);
          }
          sso = null;             
    } 
}
