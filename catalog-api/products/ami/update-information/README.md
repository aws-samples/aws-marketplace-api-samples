### Language specific examples
For examples of this Change type in one of the language-specific AWS SDKs, see the following:
- [AWS Command Line Interface](example.sh)

### Request Parameters for UpdateInformation Change Type  

**ProductTitle (string)**  
*Optional. Updated name of the product to be displayed to customers*  
  
**ShortDescription (string)**  
*Optional. Brief description of key aspects of the product to be displayed to customers*  
  
**LongDescription (string)**  
*Optional. Detailed description of the product to be displayed to customers*  
  
**Sku (string)**  
*Optional. Free-form string field for sellers to define and use as a reference to product in their own catalog*  
  
**LogoUrl (string)**  
*Optional. S3 link to an image to be displayed to customers in `PNG` or `JPG` format. The image should be hosted in a public s3 bucket or a pre-signed URL to the image hosted in a private bucket*  
  
**VideoUrls (string list)**  
*Optional. List of links to an externally hosted video to be provided as reference to customers on Marketplace website* 
  
**Highlights (string list)**  
*Optional. Short bullet style call outs of key product features*  
  
**AdditionalResources (structure list)**  
*Optional. References to additional resources to learn about the product to be displayed to customers*  
  
 - **Text (string)**  
*Displayed as text to corresponding hyperlink*  
  
 - **Url (string)**  
*URL for the corresponding resource text*
  
**SupportDescription (string)**  
*Optional. Contact details and URLs to reach the support group of the product if support is offered*  
  
**Categories (string list)**  
*Optional. Category for your product selected from a pre-defined list of product categories identified by Marketplace*  
  
**SearchKeywords (string list)**  
*Optional. Additional keywords for the product for search experience. Seller and Product name along with product categories are automatically added for search*
