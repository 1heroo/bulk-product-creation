import uvicorn
from fastapi import FastAPI
from source.creation_management.routes import router as cc_router


app = FastAPI(title='Создание Карточек Wb')

app.include_router(router=cc_router)


if __name__ == '__main__':
    uvicorn.run(
        app='main:app',
        host='0.0.0.0',
        port=8000,
        reload=True
    )
