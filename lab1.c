#include <stdio.h>
#include <windows.h>
#include <time.h>
#include <unistd.h>


const int ALLOC_SIZE = 160 * 1024 * 1024;
const unsigned long ALLOC_ADRESS = 0x41B06F6;
const int FILL_THREADS_AMOUNT = 91;
const int FILE_SIZE = 33 * 1024 * 1024;
const int IO_BLOCK_SIZE = 126;
const int READ_THREADS_AMOUNT = 142;


typedef struct fillData {
    unsigned char * a_ptr;
    int thread_i;
    BOOL * flags;
} FILLDATA, * PFILLDATA;

typedef struct writeData {
    HANDLE file;
    int file_i;
    int thread_i;
    unsigned char * take_from_ptr;
    BOOL * flags;
    HANDLE * mutex;
} WRITEDATA, * PWRITEDATA;

typedef struct readData {
    HANDLE file;
    int file_i;
    int thread_i;
    int * tries;
    unsigned char * max_number;
    int * read_threads;
    HANDLE * mutex;
} READDATA, *PREADDATA;


_Noreturn DWORD WINAPI FillWithRand(LPVOID lp_data) {
    PFILLDATA data = (FILLDATA *) lp_data;

    srand((unsigned int) time(NULL) + (unsigned int) data->thread_i);

    unsigned char * start = (data->a_ptr) + (data->thread_i);
    unsigned char * end = (data->a_ptr) + (ALLOC_SIZE / sizeof(unsigned char));
    unsigned char * ptr = start;

    while (TRUE) {
        *ptr = (unsigned char) (rand() % 256);

        ptr += FILL_THREADS_AMOUNT;
        if (ptr >= end) {
            data->flags[data->thread_i] = TRUE;
            ptr = start;
        }
    }
}

_Noreturn DWORD WINAPI WriteInFile(LPVOID write_data) {
    PWRITEDATA data = (WRITEDATA * ) write_data;

    srand((unsigned long) time(NULL) + (unsigned long) data->thread_i);

    OVERLAPPED ol;
    ol.OffsetHigh = 0;
    ol.Offset = 0;
    ol.hEvent = NULL;

    while (TRUE) {
        // Выбираем случайный блок из памяти и записываем в файл
        unsigned char * ptr;
        do {
            ptr = data->take_from_ptr + (int) ((double) rand() / RAND_MAX * ALLOC_SIZE / IO_BLOCK_SIZE);
        } while (ptr + IO_BLOCK_SIZE > data->take_from_ptr + ALLOC_SIZE);

        WaitForSingleObject(data->mutex[data->file_i], INFINITE);
        LockFile(data->file, ol.Offset, 0, IO_BLOCK_SIZE, 0);

        WriteFile(data->file, ptr, (DWORD) IO_BLOCK_SIZE, NULL, &ol);

        UnlockFile(data->file, ol.Offset, 0, IO_BLOCK_SIZE, 0);
        ReleaseMutex(data->mutex[data->file_i]);

        ol.Offset += IO_BLOCK_SIZE;
        if (ol.Offset > FILE_SIZE) {
            data->flags[data->file_i] = TRUE;
            ol.Offset = 0;
        }
    }
}

DWORD WINAPI ReadFromFile(LPVOID readData) {
    PREADDATA data = (PREADDATA) readData;

    srand((unsigned long) time(NULL) + (unsigned long) data->thread_i);

    LPVOID read_buffer = VirtualAlloc(NULL, IO_BLOCK_SIZE, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);

    OVERLAPPED ol;
    ol.OffsetHigh = 0;
    ol.Offset = 0;
    ol.hEvent = NULL;

    while (TRUE) {
        // Выбираем случайный блок для чтения, который не выходит за границы файла
        do {
            ol.Offset = (int) ((double) rand() / RAND_MAX * FILE_SIZE / IO_BLOCK_SIZE) * IO_BLOCK_SIZE;
        } while (ol.Offset + IO_BLOCK_SIZE > FILE_SIZE);

        WaitForSingleObject(data->mutex[data->file_i], INFINITE);
        LockFile(data->file, ol.Offset, 0, IO_BLOCK_SIZE, 0);

        ReadFile(data->file, read_buffer, IO_BLOCK_SIZE, NULL, &ol);

        UnlockFile(data->file, ol.Offset, 0, IO_BLOCK_SIZE, 0);
        ReleaseMutex(data->mutex[data->file_i]);

        // Ищем максимум
        unsigned char * ptr = (unsigned char *) read_buffer;
        unsigned char * end = ptr + (IO_BLOCK_SIZE / sizeof(unsigned char));

        unsigned char buffer_max = 0;
        for (; ptr != end; ++ptr) {
            if (*ptr > buffer_max)
                buffer_max = *ptr;
        }
        if (buffer_max > data->max_number[data->file_i])
            data->max_number[data->file_i] = buffer_max;

        // Завершаем цикл, если достигли нужного кол-ва попыток чтения на этот файл
        data->tries[data->file_i]++;

        if (data->tries[data->file_i] >= 2 * FILE_SIZE/IO_BLOCK_SIZE) {
            (*(data->read_threads))--;

            VirtualFree(read_buffer, 0, MEM_RELEASE);
            return 0;

        }
    }
}

int main() {
    int a;
    printf("Write any number: ");
    scanf("%d", &a);
    // ---------- Аллокация и заполнение памяти ----------

    // Выделяем область памяти в нужном нам адресе
    LPVOID alloc_ptr = VirtualAlloc((LPVOID) ALLOC_ADRESS, ALLOC_SIZE , MEM_RESERVE, PAGE_READWRITE);

    // Если не получилось, выделяем где получится
    if (alloc_ptr != NULL)
        printf("Memory allocated at 0x%X with a great success!\n",(unsigned int) alloc_ptr);
    else {
        alloc_ptr = VirtualAlloc(NULL, ALLOC_SIZE , MEM_RESERVE, PAGE_READWRITE);
        printf("Memory allocated at 0x%X instead\n", (unsigned int) alloc_ptr);
    }

    VirtualAlloc(alloc_ptr, ALLOC_SIZE, MEM_COMMIT, PAGE_READWRITE);

    // Создаем потоки, заполняющие случайными числами выделенную память.
    printf("Creating fill threads... ");

    HANDLE fill_threads[FILL_THREADS_AMOUNT];
    PFILLDATA fill_data[FILL_THREADS_AMOUNT];
    BOOL fill_flags[FILL_THREADS_AMOUNT];
    for (int i = 0; i < FILL_THREADS_AMOUNT; i++) {
        fill_data[i] = (PFILLDATA) HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(FILLDATA));
        fill_data[i]->a_ptr = ((unsigned char *) alloc_ptr);
        fill_data[i]->thread_i = i;

        fill_flags[i] = FALSE;
        fill_data[i]->flags = fill_flags;

        fill_threads[i] = CreateThread(NULL, 0,
                                       FillWithRand, fill_data[i], 0, NULL);
    }
    printf("All fill threads are created! ");

    //Ждем, пока память не будет заполнена хотя бы 1 раз
    for (int i = 0; i < FILL_THREADS_AMOUNT; i++) {
        while (fill_flags[i] == FALSE) { }
    }
    printf("Memory is filled at least once.\n");

    // ---------- Создание и запись файлов ----------

    // Создаем файлы
    int files_amount = ALLOC_SIZE / FILE_SIZE + (ALLOC_SIZE % FILE_SIZE != 0 ? 1 : 0);
    HANDLE files[files_amount];

    printf("Generating %d files... ", files_amount);
    for (int i = 0; i < files_amount; i++) {
        // Имя файла — его номер и расширение txt
        char file_name[20];
        sprintf(file_name, "%d", i);
        strcat(file_name, ".txt");

        files[i] = CreateFileA(
                file_name,
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_WRITE | FILE_SHARE_READ,
                NULL,
                CREATE_ALWAYS,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED | FILE_FLAG_RANDOM_ACCESS,
                NULL
        );
    }
    printf("Files generated! \n");

    // Для каждого файла создаем Mutex-объект, который будем крутить при записи/чтении
    HANDLE file_mutex[files_amount];
    for (int i = 0; i < files_amount; i++)
        file_mutex[i] = CreateMutexA(NULL, FALSE, NULL);

    // Создаем потоки записи и подготавливаем данные для них
    printf("Creating write threads... ");

    HANDLE write_threads[files_amount];
    PWRITEDATA write_data[files_amount];
    BOOL write_flags[files_amount];
    for (int i = 0; i < files_amount; i++) {
        write_data[i] = (PWRITEDATA) HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(WRITEDATA));
        write_data[i]->file = files[i];
        write_data[i]->take_from_ptr = (unsigned char *) alloc_ptr;
        write_data[i]->file_i = i;
        write_data[i]->thread_i = i;

        write_flags[i] = FALSE;
        write_data[i]->flags = write_flags;
        write_data[i]->mutex = file_mutex;

        write_threads[i] = CreateThread(NULL, 0,
                                        WriteInFile, write_data[i], 0, NULL);
    }
    printf("All write threads a created! ");

    //for (int i = 0; i < files_amount; i++)
    //    SetThreadPriority(write_threads[i], THREAD_PRIORITY_ABOVE_NORMAL);

    // Ждем, пока файлы не запишутся хотя бы 1 раз
    for (int i = 0; i < files_amount; i++) {
        while (write_flags[i] == FALSE) { }
    }

    //for (int i = 0; i < files_amount; i++)
        //SetThreadPriority(write_threads[i], THREAD_PRIORITY_NORMAL);

    printf("Files are filled at least once.\n");

    // ---------- Чтение и агрегация данных из файлов ----------

    // Подгатавливаем потоки для чтения
    printf("Creating read threads... ");

    int active_read_threads = READ_THREADS_AMOUNT;
    int read_tries[files_amount];
    unsigned char max_number[files_amount];
    for (int i = 0; i < files_amount; ++i) {
        read_tries[i] = 0;
        max_number[i] = 0;
    }

    HANDLE read_threads[READ_THREADS_AMOUNT];
    PREADDATA read_data[READ_THREADS_AMOUNT];
    for (int i = 0; i < READ_THREADS_AMOUNT; ++i) {
        read_data[i] = (PREADDATA) HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, sizeof(READDATA));

        // Равномерно распределяем файлы между потоками
        read_data[i]->file = files[i % files_amount];
        read_data[i]->file_i = i % files_amount;

        read_data[i]->thread_i = i;
        read_data[i]->tries = read_tries;
        read_data[i]->max_number = max_number;
        read_data[i]->read_threads = &active_read_threads;

        read_data[i]->mutex = file_mutex;

        read_threads[i] = CreateThread(NULL, 0, ReadFromFile, read_data[i], 0, NULL);
    }
    printf("All read threads are created! ");

    //for (int i = 0; i < READ_THREADS_AMOUNT; i++)
       //SetThreadPriority(read_threads[i], THREAD_PRIORITY_ABOVE_NORMAL);

    // Ждём, пока потоки дочитают каждый файл нужное кол-во раз и выводим результат
    while (active_read_threads > 0) { }
    printf("Attempts number is satisfying.\n\n");

    //for (int i = 0; i < READ_THREADS_AMOUNT; i++)
        //SetThreadPriority(read_threads[i], THREAD_PRIORITY_NORMAL);

    for (int i = 0; i < files_amount; i++) {
        printf("Max number in file %d.txt is %d\n", i, (int) max_number[i]);
    }
    printf("\n");


   // ---------- Закрытие потоков и очистка памяти ----------

    for (int i = 0; i < FILL_THREADS_AMOUNT; i++) {
        TerminateThread(fill_threads[i], 0);
        CloseHandle(fill_threads[i]);
    }
    printf("Fill threads closed.\n");

    for (int i = 0; i < files_amount; i++) {
        TerminateThread(write_threads[i], 0);
        CloseHandle(write_threads[i]);
    }
    printf("Writing threads closed.\n");

    for (int i = 0; i < READ_THREADS_AMOUNT; i++) {
        CloseHandle(read_threads[i]);
    }
    printf("Read threads closed.\n");

    for (int i = 0; i < files_amount; i++) {
        CloseHandle(file_mutex[i]);
    }
    printf("Mutexes closed.\n");


    VirtualFree(alloc_ptr, 0, MEM_RELEASE);
    printf("Memory released.\n");

    printf("Write any number: ");
    scanf("%d", &a);
    return 0;
}
