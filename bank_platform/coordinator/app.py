"""2PC coordinator process scaffold; durable state comes in later phases."""
from fastapi import FastAPI
from fastapi.responses import JSONResponse


def create_app() -> FastAPI:
    app = FastAPI(title="Distributed Transaction Coordinator", version="0.1.0")

    @app.get("/health")
    def health() -> dict:
        return {"status": "alive", "service": "coordinator", "transactions_ready": False}

    @app.post("/internal/transactions")
    def transactions() -> JSONResponse:
        return JSONResponse(status_code=501, content={
            "status": "error", "error": {"code": "NOT_IMPLEMENTED", "message": "2PC is not ready"}
        })

    return app


app = create_app()
