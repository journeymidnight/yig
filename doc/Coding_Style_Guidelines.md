- Line width should be roughly less than 80 characters  
- Use UNDERSCORE_CAPITAL_STYLE for constants and global variables, camelCase for method names and other variables  
- Do not use abbreviations for variables that appear in more than one method. e.g.  
   &#8195;prefer:  
   &#8195;&#8195;bucketName  
   &#8195;to:  
   &#8195;&#8195;bname  
   &#8195;&#8195;bktName  

  &#8195;&#8195;Some well-known abbreviations, like `max`, `min`, `info`, though, could still be used.    
  &#8195;&#8195;Generally perfer long variable names to short ones. We do not need to save those few bytes like in 1980's  
  
- Use Go style parameter passing    
   &#8195;prefer:  
   &#8195;&#8195;func hehe() (result, error) {}  
   &#8195;to:  
   &#8195;&#8195;func hehe(result *T) error {}  
      
- Use `go fmt` to format your code before commit  
- Acronyms should generally be in lower case, e.g.  
   &#8195;prefer:  
   &#8195;&#8195;userId  
   &#8195;&#8195;getCpuInfo  
   &#8195;to:  
   &#8195;&#8195;userID  
   &#8195;&#8195;getCPUInfo  

- Handle errors as soon as possible and handle EVERY error  
- Use comment markers like `TODO` `HACK` `NOTE` to mark code that needs further pondering. e.g.  
   &#8195;// TODO: use a faster algorithm  