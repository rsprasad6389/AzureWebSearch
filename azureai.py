import streamlit as st
from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient
import os
from dotenv import load_dotenv

# ---------------------------
# Load environment variables
# ---------------------------
load_dotenv()

PROJECT_ENDPOINT = os.getenv("AZURE_PROJECT_ENDPOINT")

# ---------------------------
# Streamlit page setup
# ---------------------------
st.set_page_config(page_title="ABM Web Search Agent", page_icon="🔎", layout="wide")
st.title("🔎 ABM Web Search Agent")
st.caption("Ask any question. The agent can browse the web and answer.")

# ---------------------------
# Cache client
# ---------------------------
@st.cache_resource
def get_clients():
    project_client = AIProjectClient(
        endpoint=PROJECT_ENDPOINT,
        credential=DefaultAzureCredential(),
    )
    openai_client = project_client.get_openai_client()
    return project_client, openai_client

project_client, openai_client = get_clients()

# ---------------------------
# Chat history
# ---------------------------
if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# ---------------------------
# User input
# ---------------------------
question = st.chat_input("Type your question here...")

if question:
    # Show user message
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    # Get assistant response
    with st.chat_message("assistant"):
        with st.spinner("Searching the web and thinking..."):
            try:
                response = openai_client.responses.create(
                    input=question,
                    extra_body={
                        "agent_reference": {
                            "name": "ABMWebSearchAgent",
                            "version": "4",
                            "type": "agent_reference"
                        }
                    }
                )

                answer = response.output_text

                st.markdown(answer)
                st.session_state.messages.append(
                    {"role": "assistant", "content": answer}
                )

            except Exception as e:
                error_msg = f"Error: {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append(
                    {"role": "assistant", "content": error_msg}
                )
