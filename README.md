### DISTRINFER
 

If you have a large bulk of data that needs to be run through some models, this is the tool for it. Due to the size of data, Scaling the inference layer would be the best way to increase efficiency. This is the core idea of this project.
As of now, The project uses FastAPI, Llama.CPP and Celery, along with RabbitMQ and Postgres as dependencies


### Initial Results

  
Test were conducted as 100 prompts, Through the same AI model (Qwen 2.5 0.5B) with max_tokens=512


###### - R5 3400G (Celery) -> 15:21:71

	 - Baseline for the distributed System, if lower timing in combined, it works

  

###### - Ryzen 5 3400G + Core i3 6006U (Celery) -> 11:43:02

	- My system works, but current system not efficient to make a significant difference, Still a Win though