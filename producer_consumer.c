#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#define BUFFER_SIZE 5

char buffer[BUFFER_SIZE];
int in = 0, out = 0, count = 0;
int done = 0;

pthread_mutex_t lock;
pthread_cond_t not_full;
pthread_cond_t not_empty;

void *producer(void *arg)
{
    FILE *fp = fopen("message.txt", "r");
    char ch;

    if(fp == NULL){
        printf("ERROR: can't open message.txt!\n");
        done = 1;
        pthread_cond_signal(&not_empty);
        pthread_exit(NULL);
    }

    while((ch = fgetc(fp)) != EOF){
        pthread_mutex_lock(&lock);

        while(count == BUFFER_SIZE)
            pthread_cond_wait(&not_full, &lock);

        buffer[in] = ch;
        in = (in + 1) % BUFFER_SIZE;
        count++;

        pthread_cond_signal(&not_empty);
        pthread_mutex_unlock(&lock);
    }

    fclose(fp);

    pthread_mutex_lock(&lock);
    done = 1;
    pthread_cond_signal(&not_empty);
    pthread_mutex_unlock(&lock);

    pthread_exit(NULL);
}

void *consumer(void *arg)
{
    char ch;

    while(1){
        pthread_mutex_lock(&lock);

        while(count == 0 && !done)
            pthread_cond_wait(&not_empty, &lock);

        if(count == 0 && done){
            pthread_mutex_unlock(&lock);
            break;
        }

        ch = buffer[out];
        out = (out + 1) % BUFFER_SIZE;
        count--;

        pthread_cond_signal(&not_full);
        pthread_mutex_unlock(&lock);

        putchar(ch);
        fflush(stdout);
    }

    pthread_exit(NULL);
}

int main()
{
    pthread_t prod, cons;

    pthread_mutex_init(&lock, NULL);
    pthread_cond_init(&not_full, NULL);
    pthread_cond_init(&not_empty, NULL);

    pthread_create(&prod, NULL, producer, NULL);
    pthread_create(&cons, NULL, consumer, NULL);

    pthread_join(prod, NULL);
    pthread_join(cons, NULL);

    pthread_mutex_destroy(&lock);
    pthread_cond_destroy(&not_full);
    pthread_cond_destroy(&not_empty);

    return 0;
}
