import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers.tax_router import router as tax_router
from routers.validation_router import router as validation_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(tax_router)
app.include_router(validation_router)

if __name__ == "__main__":
    uvicorn.run("main:app", port=8022, reload=True, host="0.0.0.0")