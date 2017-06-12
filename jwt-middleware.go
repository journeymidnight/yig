package main


import (
	"github.com/journeymidnight/yig/helper"
	"github.com/dgrijalva/jwt-go"
	"net/http"
	"context"
	"strings"
	"fmt"
)

type JwtMiddleware struct {
	handler         http.Handler
}

func FromAuthHeader(r *http.Request) (string, error) {

	authHeader, ok := r.Header["Authorization"]
	helper.Logger.Println(5, "authHeader:",authHeader)
	if ok == false || authHeader[0] == "" {
		return "", nil // No error, just no token
	}
	
	authHeaderParts := strings.Split(authHeader[0], " ")
	if len(authHeaderParts) != 2 || strings.ToLower(authHeaderParts[0]) != "bearer" {
		return "", fmt.Errorf("Authorization header format must be Bearer {token}")
	}

	return authHeaderParts[1], nil
}

func (m *JwtMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tokenString, err := FromAuthHeader(r)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	parsedToken, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Don't forget to validate the alg is what you expect:
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}

		// hmacSampleSecret is a []byte containing your secret, e.g. []byte("my_secret_key")
		return []byte(helper.CONFIG.AdminKey), nil
	})
	if err != nil {
		w.WriteHeader(401)
		return
	}
	if claims, ok := parsedToken.Claims.(jwt.MapClaims); ok && parsedToken.Valid {
		var userKey string = "claims"
		ctx := context.WithValue(r.Context(), userKey, claims)
		m.handler.ServeHTTP(w, r.WithContext(ctx))
		return
	} else {
		w.WriteHeader(401)
		return
	}

}

func SetJwtMiddlewareHandler(handler http.Handler) http.Handler {
	jwtChecker := &JwtMiddleware{
		handler:         handler,
	}
	return jwtChecker
}

func SetJwtMiddlewareFunc(f func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	jwtChecker := &JwtMiddleware{
		handler:         http.HandlerFunc(f),
	}
	return jwtChecker.ServeHTTP
}

