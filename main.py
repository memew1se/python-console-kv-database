from copy import copy


class Command:
    """
    Класс содержит строковые константы, представляющих различные команды,
    которые могут использоваться в базе данных
    """

    SET = "SET"
    GET = "GET"
    UNSET = "UNSET"
    COUNTS = "COUNTS"
    FIND = "FIND"
    END = "END"
    BEGIN = "BEGIN"
    ROLLBACK = "ROLLBACK"
    COMMIT = "COMMIT"


class CommandLength:
    """
    Класс содержит числовые константы, представляющие количество аргументов,
    необходимых для каждой команды в базе данных
    """

    SET = 3
    GET = 2
    UNSET = 2
    COUNTS = 2
    FIND = 2
    END = 1
    BEGIN = 1
    ROLLBACK = 1
    COMMIT = 1


class ExitException(Exception):
    """
    Исключение, которое возникает при попытке выхода из программы
    """

    pass


class CommandValidationException(Exception):
    """
    Исключение, которое возникает, когда операция имеет неизвестную команду
    или неправильное количество аргументов
    """


class TransactionRollbackException(Exception):
    """
    Исключение, которое возникает, когда выполняется откат
    транзакции в базе данных
    """

    pass


# Словарь для проверки команды на правильное количество аргументов
operation_command_length = {}
operation_command_length[Command.SET] = CommandLength.SET
operation_command_length[Command.GET] = CommandLength.GET
operation_command_length[Command.UNSET] = CommandLength.UNSET
operation_command_length[Command.COUNTS] = CommandLength.COUNTS
operation_command_length[Command.FIND] = CommandLength.FIND
operation_command_length[Command.END] = CommandLength.END
operation_command_length[Command.BEGIN] = CommandLength.BEGIN
operation_command_length[Command.ROLLBACK] = CommandLength.ROLLBACK
operation_command_length[Command.COMMIT] = CommandLength.COMMIT


def validate_operation(operation: list[str]) -> None:
    """
    Проверяет корректность команды и её аргументов, основываясь на заданных правилах

    Параметры:
    ----------
    operation : list[str]
        Список строк, представляющий команду и её аргументы. Первый элемент списка — это команда, остальные элементы — её аргументы

    Логика работы:
    --------------
    1. Проверяется, что список команд `operation` не пуст
    2. Первый элемент списка `operation` (команда) преобразуется в верхний регистр
    3. Проверяется наличие команды в словаре `operation_command_length`
    4. Сравнивается количество аргументов команды с ожидаемым значением
    5. Если проверка успешна, функция завершает выполнение.
    6. Если команда не найдена или количество аргументов неверно, выбрасывается исключение `CommandValidationException`
    """

    if operation:
        command = operation[0].upper()
        if command in operation_command_length:
            if operation_command_length[command] == len(operation):
                return
    raise CommandValidationException


def operate(
    database: dict[str, str] | None = None, is_transaction: bool = False
) -> dict[str, str]:
    """
    Выполняет операции над базой данных в интерактивном режиме, поддерживая транзакции

    Параметры:
    ----------
    database : dict[str, str] | None, optional
        База данных в виде словаря. Если не передана, создается новая пустая база данных
    is_transaction : bool, optional
        Флаг, указывающий, выполняется ли операция в рамках транзакции. По умолчанию False

    Возвращаемое значение:
    ----------------------
    dict[str, str]
        База данных после выполнения всех операций, либо состояние базы на момент завершения транзакции

    Исключения:
    -----------
    ExitException
        Выбрасывается при завершении работы через команду END или при прерывании программы
    CommandValidationException
        Выбрасывается при вводе некорректной команды или при неверном количестве аргументов
    TransactionRollbackException
        Выбрасывается при откате транзакции

    Команды:
    --------
    - SET <key> <value>: Устанавливает значение `value` для ключа `key` в базе данных
    - GET <key>: Выводит значение, соответствующее ключу `key`. Если ключ не найден — выводит "NULL"
    - UNSET <key>: Удаляет ключ `key` из базы данных
    - COUNTS <value>: Выводит количество ключей, имеющих значение `value`
    - FIND <value>: Выводит ключи, соответствующие значению `value`
    - END: Завершает программу
    - BEGIN: Начинает новую транзакцию
    - ROLLBACK: Откатывает все изменения, сделанные в текущей транзакции
    - COMMIT: Завершает текущую транзакцию, сохраняя изменения

    Логика работы:
    --------------
    1. Если база данных не передана, создается пустая база данных
    2. Функция работает в бесконечном цикле, ожидая ввода команды от пользователя
    3. Полученная команда очищается от лишних пробелов и валидируется через `validate_operation`
    4. В зависимости от команды выполняются соответствующие действия с базой данных
    5. При команде `BEGIN` начинается новая транзакция, которая может быть отменена (ROLLBACK) или завершена (COMMIT)
    6. Если программа прерывается (например, через `Ctrl+C` или команду `END`), выбрасывается исключение `ExitException`
    7. Если команда неверная или имеет неправильное количество аргументов, выводится сообщение об ошибке, и цикл продолжается
    """

    if database is None:
        database = {}

    while True:
        try:
            raw_operation = input("> ")
        except (EOFError, KeyboardInterrupt):
            raise ExitException

        # Убираем лишние пробелы
        operation = list(filter(lambda x: x != "", raw_operation.split(" ")))

        try:
            validate_operation(operation)
        except CommandValidationException:
            print("Неизвестная команда или неправильное количество аргументов")
            continue

        command = operation[0].upper()
        match command:
            case Command.SET:
                key = operation[1]
                value = operation[2]
                database[key] = value

            case Command.GET:
                key = operation[1]
                value = database.get(key, "NULL")
                print(value)

            case Command.UNSET:
                key = operation[1]
                database.pop(key, "")

            case Command.COUNTS:
                counter = 0
                value = operation[1]
                for v in database.values():
                    if v == value:
                        counter += 1
                print(counter)

            case Command.FIND:
                value = operation[1]
                commands = []
                for k, v in database.items():
                    if v == value:
                        commands.append(k)
                print(", ".join(commands))

            case Command.END:
                raise ExitException

            case Command.BEGIN:
                try:
                    database = operate(copy(database), True)
                except TransactionRollbackException:
                    pass

            case Command.ROLLBACK:
                if not is_transaction:
                    print("Нет текущей транзакции")
                    continue
                raise TransactionRollbackException

            case Command.COMMIT:
                if not is_transaction:
                    print("Нет текущей транзакции")
                    continue
                break
    return database


if __name__ == "__main__":
    try:
        operate()
    except ExitException:
        pass
