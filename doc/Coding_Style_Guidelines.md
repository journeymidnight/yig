- Line width should be roughly less than 80 characters  
- Use UNDERSCORE_CAPITAL_STYLE for constants and global variables, camelCase for method names and other variables  
- Do not use abbreviations for variables that appear in more than one method. e.g.

        prefer:
            bucketName
        to:
            bname
            bktName

    Some well-known abbreviations, like `max`, `min`, `info`, though, could still be used.    
    Generally perfer long variable names to short ones. We do not need to save those few bytes like in 1980's  
  
- Use Go style parameter passing    

        prefer:  
            func hehe() (result, error) {}  
        to:  
            func hehe(result *T) error {}  
      
- Use `go fmt` to format your code before commit  
- Acronyms should generally be in lower case, e.g.  

        prefer:  
            userId  
            getCpuInfo  
        to:  
            userID  
            getCPUInfo  

- Handle errors as soon as possible and handle EVERY error  
- Use comment markers like `TODO` `HACK` `NOTE` to mark code that needs further pondering. e.g.  

        // TODO: use a faster algorithm  
