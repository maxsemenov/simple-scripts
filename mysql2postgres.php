<?php

ini_set('memory_limit', '768M');
set_time_limit(0);

$mysqlHost = '';
$mysqlDb = '';
$mysqlUser = '';
$mysqlPassword = '';

$tables = <<<TABLES
table1
table2
TABLES;

$mysqlTables = explode("\n", $tables);

$postgresHost = '';
$postgresDb = '';
$postgresUser = '';
$postgresPassword = '';
$postgresTable = '';

$ignoredColumns = ['id', 'articleBodyHtml'];

try {
    $mysqlConnection = new PDO("mysql:host=$mysqlHost;dbname=$mysqlDb;charset=utf8", $mysqlUser, $mysqlPassword);
    $mysqlConnection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $postgresConnection = new PDO("pgsql:host=$postgresHost;dbname=$postgresDb;user=$postgresUser;password=$postgresPassword");
    $postgresConnection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    foreach ($mysqlTables as $mysqlTable) {

        $offset = 0;
        $batchSize = 300;

        do {
            $mysqlQuery = $mysqlConnection->query("SELECT * FROM $mysqlTable LIMIT $offset, $batchSize");
            $data = $mysqlQuery->fetchAll(PDO::FETCH_ASSOC);
            
            foreach ($data as &$row) {
                array_walk($row, function (&$value) { $value = mb_convert_encoding($value, 'UTF-8', 'UTF-8'); });
                foreach ($ignoredColumns as $column) { unset($row[$column]); }
                $columns = implode(',', array_keys($row));
                $placeholders = implode(',', array_fill(0, count($row), '?'));

                $query = "INSERT INTO $postgresTable ($columns) VALUES ($placeholders)";
                $statement = $postgresConnection->prepare($query);
                $statement->execute(array_values($row));
                $statement->closeCursor();
            }

            $offset += $batchSize;

        } while (count($data) === $batchSize);
        
        echo "Перенос данных из MySQL: $mysqlTable в PostgreSQL: $postgresTable завершен<br>";
    }

    echo "Импорт данных успешно завершен.";

} catch (PDOException $e) {
    echo "Ошибка: " . $e->getMessage();
}

?>
