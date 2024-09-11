# Top Songs Service
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import text_keyword_searcher

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)


@app.get("/api")
async def root():
    return {
        "message": "Welcome to the Top Songs API."
    }

app.include_router(text_keyword_searcher.router, prefix="/api")


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
