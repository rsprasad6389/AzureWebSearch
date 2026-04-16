from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient
import streamlit as st
import os 
from dotenv import load_dotenv

load_dotenv()

my_endpoint = os.getenv("AZURE_PROJECT_ENDPOINT")

project_client = AIProjectClient(
    endpoint=my_endpoint,
    credential=DefaultAzureCredential(),
)

my_agent = "ABMWebSearchAgent"
my_version = "4"

print(project_client)
openai_client = project_client.get_openai_client()
print(openai_client)

# Reference the agent to get a response
response = openai_client.responses.create(
    input=[{"role": "user", "content": "Tell me what you can help with."}],
    extra_body={"agent_reference": {"name": my_agent, "version": my_version, "type": "agent_reference"}},
)

st.write(f"Response output: {response.output_text}")
