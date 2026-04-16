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
try:
    response = openai_client.responses.create(
        input="Tell me what you can help with.",
        extra_body={
            "agent_reference": {
                "name": "ABMWebSearchAgent",
                "version": "4",
                "type": "agent_reference"
            }
        }
    )
    st.write(response.output_text)

except Exception as e:
    st.error(str(e))
    import traceback
    st.text(traceback.format_exc())
st.write(f"Response output: {response.output_text}")
