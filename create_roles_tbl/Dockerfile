# Create a slim image (stage 1)
FROM python:3.8 AS builder
#COPY requirements.txt .

# install dependencies to the local user directory (eg. /root/.local)
#RUN pip3 install --user -r requirements.txt

RUN pip3 install --user openpyxl
RUN pip3 install --user pandas
RUN pip3 install --user numpy
RUN pip3 install --user boto3
RUN pip3 install --user argparse


# building slim stage
FROM python:3.8-slim
WORKDIR ./src

# copy only the dependencies installation from the 1st stage image
COPY --from=builder /root/.local /root/.local
#copy everying from src directoryCOPY ./src .
COPY ./src .

# update PATH environment variable
ENV PATH=/root/.local:$PATH

#identify name etc running
ENTRYPOINT [ "python", "./roles_proccess.py" ]
