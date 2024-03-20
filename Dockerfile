# Use the official Python image
FROM python

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Copy the dependencies file to the working directory
COPY requirements.txt requirements.txt
COPY app.py /app

# Install dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy the SQLite database and JSON data into the Docker image
COPY movie.db /app/movies.db
COPY movies.json /app/movies.json

# Expose port 5000
EXPOSE 5000

# Command to run the application
CMD [ "python", "app.py" ]
