from pydantic import BaseModel


class Car(BaseModel):
    company_name: str #Компания
    url: str # Ссылка
    brand: str # Брэнд
    model: str # Модель
    year: int # Год
    price: int # Цена
    city: str # Город
    volume: float # Двигатель
    volume_type: str # Двигатель тип
    mileage: float # Пробег
    transmission: float # Трансмиссия
    custom_kz: bool # Растоможен