### Language specific examples
For examples of this Change type in one of the language-specific AWS SDKs, see the following:
- [AWS Command Line Interface](example.sh)

### Request Parameters for AddDeliveryOptions Change Type  
  
**Version (structure)**  
 *Details about the software version*
  
 - **VersionTitle (string)**    
   *Title of the new product version. Version title is a unique identifier of a version. Displayed to customers on the product detail and configuration pages*  
 
  - **ReleaseNotes (string)**  
    *Notes regarding changes in the version*  

**DeliveryOptions (list)**  
  *List of DeliveryOption objects with details about the delivery options of the version* 
  
 -  **Details (structure)**  
  *AMI Delivery option details*  
  
    - **AmiDeliveryOptionDetails (structure)**      
      *Details of AMI DeliveryOption*
      
        - **AmiSource (structure)**  
         *AMI asset and asset related metadata*  

             - **AmiId (string)**  
             *Source AMI ID located in the region where CatalogMarketplace is called and should be owned by the caller account. Used by Marketplace to make regional copies and share with entitled customers*
             
             - **AccessRoleArn (string)**  
             *IAM role ARN used by Marketplace to access the provided AmiId. Ensure its trust relationship contains public service principal for asset services: `assets.marketplace.amazonaws.com` and its policy permissions include ec2 permissions to clone the AMI within MP. The user should also have permission to pass the role to Marketplace. The AccessRole should be in the same account as the AMI*   
              
             - **UserName (string)**  
             *Login username used to access the OS; i.e. ec2-user, ubuntu, Administrator. Used for scanning the provided AMI for vulnerabilities*
               
             - **ScanningPort (integer)**  
             *Optional SSH or RDP port used to access the OS. Used for scanning the provided AMI for vulnerabilities. Defaults to 22* 
              
             - **OperatingSystemName (string)**  
               *Operating system displayed to customers*
                 
             - **OperatingSystemVersion (string)**  
                *Operating system version displayed to customers*   
          
	        - **UsageInstructions (string)**  
	         *Information on how an end user can launch the product. Displayed to end users on the product detail and fulfillment pages*
	           
	         - **AccessEndpointUrl (structure)**  
		    *Optional object for url details to a web interface for the software*
		      
	             - **Port (string)**  
	               *For products with a UI interface, enter the port of the endpoint `https://<public-dns>/relativePath:port` e.g. 80, 443*
	                 
	             - **Protocol (string)**  
                       *For products with a UI interface, enter the protocol of the endpoint `https://<public-dns>/relativePath:port` . Possible values: http | https*
                         
                  - **RelativePath (string)**  
                    *Optional. For products with a browser interface, enter the relativePath of the endpoint `https://<public-dns>/relativePath:port`*
  
	         - **RecommendedInstanceType (string)**  
                   *Default instance type the version will use with 1-Click launch and would be recommended to customers*  

	         - **SecurityGroups (list)**  
                   *List of security group objects. Ingress rules for the automatically created groups for the version*   
          
	            - **FromPort (integer)**  
	             *The source port*
	             
	             - **IpProtocol (string)**  
	              *The IP protocol. Possible values: tcp | udp*   
              
	             - **IpRanges (list)**  
	               *List of Strings - CIDR IP ranges*
	                 
	             - **ToPort (integer)**  
	               *The destination port*
            
