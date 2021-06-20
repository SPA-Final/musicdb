package main

import (
	"expvar"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

func (app *application) routes() http.Handler {
	router := httprouter.New()

	router.NotFound = http.HandlerFunc(app.notFoundResponse)
	router.MethodNotAllowed = http.HandlerFunc(app.methodNotAllowedResponse)

	router.HandlerFunc(http.MethodGet, "/v1/healthcheck", app.healthcheckHandler)

	router.HandlerFunc(http.MethodGet, "/v1/musics", app.listMusicsHandler)
	router.HandlerFunc(http.MethodGet, "/v1/musics/:id", app.showMusicHandler)
	router.HandlerFunc(http.MethodPost, "/v1/musics", app.requirePermission("musics:write", app.createMusicHandler))
	router.HandlerFunc(http.MethodPatch, "/v1/musics/:id", app.requirePermission("musics:write", app.updateMusicHandler))
	router.HandlerFunc(http.MethodDelete, "/v1/musics/:id", app.requirePermission("musics:write", app.deleteMusicHandler))

	router.HandlerFunc(http.MethodPost, "/v1/users", app.registerUserHandler)
	router.HandlerFunc(http.MethodPut, "/v1/users/activate", app.activateUserHandler)

	router.HandlerFunc(http.MethodPost, "/v1/tokens/authentication", app.createAuthenticationTokenHandler)

	router.Handler(http.MethodGet, "/v1/metrics", expvar.Handler())

	return app.metrics(app.recoverPanic(app.enableCORS(app.rateLimit(app.authenticate(router)))))
}
