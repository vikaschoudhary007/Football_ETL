import httpx
import asyncio
import os
from dotenv import load_dotenv

# Top 50 tracks - time_range - max goes to 1 year
# Top 50 Artists for user
# Get the current user's followed artists.
# Get a list of the songs saved in the current Spotify user's 'Your Music' library.
# Get a playlist owned by a Spotify user.
# Get full details of the items of a playlist owned by a Spotify user.
# Get a list of the playlists owned or followed by the current Spotify user.
# Get a list of the playlists owned or followed by a Spotify user.
# Get a list of shows saved in the current Spotify user's library. Optional parameters can be used to limit the number of shows returned.
# Get the list of objects that make up the user's queue.
# Get a list of the episodes saved in the current Spotify user's library. - beta
# Get a list of the audiobooks saved in the current Spotify user's 'Your Music' library.
# Get a list of the albums saved in the current Spotify user's 'Your Music' library.

load_dotenv()

# Replace 'your_token_here' with your actual Spotify authorization token
token = os.getenv('TOKEN')


async def fetch_web_api(endpoint, method='GET', params=None):
    url = f'https://api.spotify.com/{endpoint}'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    async with httpx.AsyncClient() as client:
        if method == 'GET':
            response = await client.get(url, headers=headers, params=params)
        elif method == 'POST':
            response = await client.post(url, headers=headers, json=params)
        elif method == 'PUT':
            response = await client.put(url, headers=headers, json=params)
        elif method == 'DELETE':
            response = await client.delete(url, headers=headers)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status()  # Raise HTTPError for bad responses
        return response.json()


async def get_top_tracks():
    # Endpoint reference: https://developer.spotify.com/documentation/web-api/reference/get-users-top-artists-and-tracks/
    endpoint = 'v1/me/top/tracks'
    params = {
        'time_range': 'long_term',
        'limit': 50
    }

    return (await fetch_web_api(endpoint, 'GET', params)).get('items', [])


# Main execution
async def main():
    try:
        top_tracks = await get_top_tracks()
        for track in top_tracks:
            artists = ', '.join([artist['name'] for artist in track['artists']])
            print(f"{track['name']} by {artists}")
    except httpx.HTTPStatusError as err:
        print(f"HTTP Error occurred: {err}")
    except Exception as e:
        print(f"Error occurred: {e}")


if __name__ == '__main__':
    asyncio.run(main())
