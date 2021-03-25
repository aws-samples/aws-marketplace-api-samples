### Language specific examples
For examples of this Change type in one of the language-specific AWS SDKs, see the following:
- [AWS Command Line Interface](example.sh)

### Request Parameters for UpdateDeliveryOptions Change Type   

**Version (structure)**    
 *Details about the software version to be updated*  
 - **ReleaseNotes (string)**    
 *Optional. Notes regarding changes in the version*   

**DeliveryOptions (list)**    
 *List of DeliveryOption objects with details about the delivery options of the version*   
    
- **Id (string)**  
  *Unique identifier for DeliveryOption*  
  
 - **Details (structure)**    
 *Details of the DeliveryOption to be updated. The delivery option title can only be changed for DeliveryOptions which have not been released to customers*    
    
   - **AmiDeliveryOptionDetails (structure)**        
 *Details of AMI DeliveryOption*  
            
		- **UsageInstructions (string)**    
 *Optional. Instructions on launching the product. Displayed to customers on the product detail and fulfillment pages. Defaults to the existing value in this version.*  

	 - **AccessEndpointUrl (structure)**    
 *Optional. Object for url details to a web interface for the software*  

		 - **Port (string)**    
		 *For products with a UI interface, enter the port of the endpoint `https://<public-dns>/relativePath:port` e.g. 80, 443*  

		 - **Protocol (string)**    
		 *For products with a UI interface, enter the protocol of the endpoint `https://<public-dns>/relativePath:port` . Possible values: http | https*  

		 - **RelativePath (string)**    
		 *Optional. For products with a browser interface, enter the relativePath of the endpoint `https://<public-dns>/relativePath:port`*  

	 - **RecommendedInstanceType (string)**    
	  *Optional. Default instance type the version will use with 1-Click launch and would be recommended to customers. Defaults to the existing value in version*   
 
	 - **SecurityGroups (list)**    
 *Optional. List of security group objects. Ingress rules for the automatically created groups for the version. Defaults to the existing value in version*     
            
		- **FromPort (integer)**    
		 *The source port*  

		 - **IpProtocol (string)**    
		 *The IP protocol. Possible values: tcp | udp*     
		                
		- **IpRanges (list)**    
		 *List of Strings - CIDR IP ranges*  

		 - **ToPort (integer)**    
		 *The destination port*
