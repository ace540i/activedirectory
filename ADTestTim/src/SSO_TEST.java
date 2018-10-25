import javax.naming.directory.Attributes;

public class SSO_TEST {

   	
    public static void main(String[] args) throws Exception
    { 
        String[] userid = {"joeblow", "tcondron", "rshur", "rodriguezr", "rosullivan", "sikudithipudi"};
  	  	String[] ssoApp = {"PENDING", "BUFFER", "PLAC", "SOMETHINGELSE"};
  	  	String username = "";
  	  	String app = "";
  	  	String cn = "";

  	  	username = userid[1];
  	  	app = ssoApp[0];
       	
/* * * * * * ** * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *  
*  First Step using USERID(network-id): 
*  	Lookup in AD (sAMAccountName=USERID) and get the 'CN' (Common Name) given for match..
*  	Then keep the CN to do lookup in SSO-application, as the sAMAccountName is not available...!!!              
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *     
*/
///////////////////////////////////////////////////////////
          SsoClass sso = new SsoClass(app);			///////
///////////////////////////////////////////////////////////           
          sso.debug=true;
          
          Attributes att = sso.get_attributes(username);
          
          if (att == null)
          {
                System.out.println("Sorry your User-id (network-id) is invalid...");
          }
          else
          {
        	  System.out.println("WELCOME: "+ "["+username+"] " + att.get("cn").toString().substring(att.get("cn").toString().indexOf(":")+2));
				
        	  cn = sso.parse_attributes();
        	  
  /* * * * * * ** * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *  
  *  Next Step using CN(Common Name) retrieved above: 
  *  	Lookup all members in the given SSO-Application and try to find a match for the 'CN' (Common Name)..
  *  	Then RETURN a Message for successful match on NOT....!!!              
  * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *     
  */        	  
		       //String msg = sso.get_memebers(cn, app);
		       int ok = sso.get_memebers(cn, app);
	
		       String msg="";
		       
		       if (ok == 0) {
		    		msg="Sorry no matching entry for: "+cn+ " in the SSO-" + app;
		       } else {
		    	 	msg="Matching entry found for: "+cn+ " in the SSO: " + app;
		       }
	          
	          System.out.println("------------------------------------------------------------------"); 
	          System.out.println("-- " + msg.toUpperCase()); 
	          System.out.println("------------------------------------------------------------------"); 

          }
          sso = null;          
          
    } //main
}
