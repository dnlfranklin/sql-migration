<?php

namespace SqlMigration\Core;

class Connector{

    private \PDO $conn;

    public function __construct(Array $info){
        $host = $info['host'] ?? null;
        $port = $info['port'] ?? null;
        $user = $info['user'] ?? null;
        $pass = $info['pass'] ?? null;

        try {
            $this->conn = new \PDO("mysql:host=$host;port=$port", $user, $pass, array(
                \PDO::ATTR_DEFAULT_FETCH_MODE =>\PDO::FETCH_ASSOC
            ));
            
        } catch (\PDOException $e) {
            throw new \Exception('Cannot connect to database.');
        }
    }

    public function get():\PDO {
        return $this->conn;        
    }

}