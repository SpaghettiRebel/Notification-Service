package api

import (
	_ "embed"
	"net/http"
)

//go:embed openapi.yaml
var openAPISpec []byte

// Like FastAPI's /docs and /redoc, the pages load their scripts from jsDelivr.
const swaggerUIPage = `<!doctype html>
<html>
<head>
  <title>Public API - Swagger UI</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>SwaggerUIBundle({url: "/openapi.yaml", dom_id: "#swagger-ui"});</script>
</body>
</html>`

const redocPage = `<!doctype html>
<html>
<head>
  <title>Public API - ReDoc</title>
</head>
<body>
  <redoc spec-url="/openapi.yaml"></redoc>
  <script src="https://cdn.jsdelivr.net/npm/redoc@2/bundles/redoc.standalone.js"></script>
</body>
</html>`

func registerDocs(mux *http.ServeMux) {
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(openAPISpec)
	})
	mux.HandleFunc("GET /docs", htmlPage(swaggerUIPage))
	mux.HandleFunc("GET /redoc", htmlPage(redocPage))
}

func htmlPage(page string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(page))
	}
}
