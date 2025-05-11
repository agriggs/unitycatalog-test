from unitycatalog.client import ApiClient, Configuration
from unitycatalog.ai.core.client import UnitycatalogFunctionClient

config = Configuration()
# This is the default address when starting a UnityCatalog server locally. Update this to the uri
# of your running UnityCatalog server.
config.host = "http://localhost:3000/api/2.1/unity-catalog"

# Create the UnityCatalog client
api_client = ApiClient(configuration=config)

# Use the UnityCatalog client to create an instance of the AI function client
client = UnitycatalogFunctionClient(api_client=api_client)

CATALOG = "enodo"
SCHEMA = "ai-test"

def add_numbers(number_1: float, number_2: float) -> float:
    """
    A function that accepts two floating point numbers, adds them,
    and returns the resulting sum as a float.

    Args:
        number_1 (float): The first of the two numbers to add.
        number_2 (float): The second of the two numbers to add.

    Returns:
        float: The sum of the two input numbers.
    """
    return number_1 + number_2

# Create the function in UC
function_info = client.create_python_function(
    func=add_numbers,
    catalog=CATALOG,
    schema=SCHEMA,
)

from unitycatalog.ai.langchain.toolkit import UCFunctionToolkit

# Define the UC function to be used as a tool
func_name = f"{CATALOG}.{SCHEMA}.add_numbers"

# Create a toolkit with the UC function
toolkit = UCFunctionToolkit(function_names=[func_name], client=client)

from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.chat_models.databricks import ChatDatabricks

# Initialize the LLM
llm = ChatDatabricks(endpoint="databricks-meta-llama-3-1-70b-instruct")

# Define the prompt
prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a helpful assistant. Use tools for computations if applicable."),
    ("placeholder", "{chat_history}"),
    ("human", "{input}"),
    ("placeholder", "{agent_scratchpad}")
])

# Create the agent with our tools
agent = create_tool_calling_agent(llm, toolkit.tools, prompt)

# Create the executor, adding our defined tools from the UCFunctionToolkit instance
agent_executor = AgentExecutor(agent=agent, tools=toolkit.tools, verbose=True)

# Run the agent with an input
agent_executor.invoke({"input": "What is the sum of 4321.9876 and 1234.5678?"})
