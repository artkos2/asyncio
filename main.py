import asyncio
import datetime
import re
import requests
from aiohttp import ClientSession
from more_itertools import chunked
from db.db import People, Session, engine, Base


CHUNK_SIZE = 5


def get_homeworld(planet: str):
    planet_id = re.search("\d+", planet).group()
    print(f'start planet {planet_id}')
    resp = requests.get(f'https://swapi.dev/api/planets/{planet_id}').json()
    print(f'end planet {planet_id}')
    return resp['name']


def get_films(films: list):
    films_str = ''
    if films:
        for film in films:
            film_id = re.search("\d+", film).group()
            print(f'start films {film_id}')
            resp = requests.get(f'https://swapi.dev/api/films/{film_id}').json()
            films_str = f'{films_str} {resp["title"]},'
        print(f'end films {film_id}')
    return films_str.rstrip(",")


def get_species(species: list):
    species_str = ''
    if species:
        for specie in species:
            specie_id = re.search("\d+", specie).group()
            print(f'start species {specie_id}')
            resp = requests.get(f'https://swapi.dev/api/species/{specie_id}').json()
            species_str = f'{species_str} {resp["name"]},'
        print(f'end species {specie_id}')
    return species_str.rstrip(",")


def get_starships(starships: list):
    starships_str = ''
    if starships:
        for starship in starships:
            starship_id = re.search("\d+", starship).group()
            print(f'start starships {starship_id}')
            resp = requests.get(f'https://swapi.dev/api/starships/{starship_id}').json()
            starships_str = f'{starships_str} {resp["name"]},'
        print(f'end starships {starship_id}')
    return starships_str.rstrip(",")


def get_vehicles(vehicles: list):
    vehicles_str = ''
    if vehicles:
        for vehicle in vehicles:
            vehicle_id = re.search("\d+", vehicle).group()
            print(f'start starships {vehicle_id}')
            resp = requests.get(f'https://swapi.dev/api/vehicles/{vehicle_id}').json()
            vehicles_str = f'{vehicles_str} {resp["name"]},'
        print(f'end starships {vehicle_id}')
    return vehicles_str.rstrip(",")


async def chunked_async(async_iter, size):
    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            if buffer:
                yield buffer
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_person(people_id: int, session: ClientSession):
    print(f'begin {people_id}')
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()
    print(f'end {people_id}')
    return json_data


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 80), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)
            for item in results:
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all([People(birth_year=item['birth_year'],
                                eye_color=item['eye_color'],
                                gender=item['gender'],
                                hair_color=item['hair_color'],
                                height=item['height'],
                                mass=item['mass'],
                                name=item['name'],
                                skin_color=item['skin_color'],
                                films=get_films(item['films']),
                                homeworld=get_homeworld(item['homeworld']),
                                species=get_species(item['species']),
                                starships=get_starships(item['starships']),
                                vehicles=get_vehicles(item['vehicles'])
                                ) for item in people_chunk])
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        asyncio.create_task(insert_people(chunk))

    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
