import tiktoken
from openai import OpenAI

def num_tokens_from_string(string: str, encoding_name: str = "cl100k_base") -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding(encoding_name)
    num_tokens = len(encoding.encode(string))
    return num_tokens





def get_embedding(text, model="bge-m3",base_url="http://localhost:6001/v1",api_key=None):
    """
    Get embeddings for the provided text using OpenAI's API
    
    Args:
        text (str): The input text to get embeddings for
        model (str): The embedding model to use
        
    Returns:
        list: The embedding vector
    """
    # Initialize the client
    # Replace with your API key or set it as an environment variable
    # Get API key and base URL from environment variables or use defaults
    
   
    # Initialize the client with custom base URL
    client = OpenAI(base_url=base_url, api_key=api_key)
    
    # Get the embedding from OpenAI
    response = client.embeddings.create(
        input=text,
        model=model
    )
    
    # Extract the embedding from the response
    embedding_results = [
        item.embedding for item in response.data
    ]
    
    return embedding_results