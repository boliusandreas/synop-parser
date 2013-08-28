<?php
error_reporting(E_ERROR | E_WARNING | E_PARSE);
class SynopImporter {

    /**
     * @var Mongo
     */
    protected $mongoConnection;

    protected $time;
    /**
     * @var array
     */
    public $use = array("06102", "06120", "06123", "06118", "06154", "06104", "06135", "06110");
    /**
     * @var array
     */
    public $bitIds = array("first", "vind", "temperature", "dugpunkt", "stationstryk", "havtryk", "trykændring");
    /**
     * @var array
     */
    public $retninger = array("n", "nø", "nø", "ø", "ø", "sø", "sø", "s", "s", "sv", "sv", "v", "v", "nv", "nv", "n"
    );

    public $dmiCollection;

    /**
     *
     */
    function __construct() {
        $mongoConnection = new Mongo();
        $this->dmiCollection = $mongoConnection->seih->dmi;
        $this->dmiCollection->ensureIndex(array('date' => 1));

    }

    /**
     * @param $directory⁄
     * @return boolean
     */
    public function importFromDirectory($directory = "/home/sites/seih/htdocs/data/passiv/2013*/DMI/") {
        print "Importing from directory: " . $directory . PHP_EOL;
        foreach ($this->rglob($directory . '*.DAT') as $file) {
            try {
                $this->importSingleFile($file);
            } catch (Exception $e) {
                print '    Error: ' . $e->getMessage() . PHP_EOL;
            }
        };
        return TRUE;
    }

    /**
     * @param $filename
     * @return void
     */
    public function importSingleFile($fileName) {
        print "  Importing from file " . $fileName;
        $titel = explode("0000_", $fileName);
        $titel = explode(".", $titel[1]);
        $year = substr($titel[0], 0, 4);
        $month = substr($titel[0], 4, 2);
        $day = substr($titel[0], 6, 2);
        $hour = substr($titel[0], 8, 2);
        $this->time = new MongoDate(strtotime($year . "-" . $month . "-" . $day . " " . $hour . ":00:00"));

        if (($handle = fopen($fileName, "r")) !== FALSE) {
            // remove empty lines
            while (($buffer = fgets($handle, 1000)) !== false) {
                if (trim($buffer)) {
                    $lines[$titel[0]][] = trim($buffer);
                }
            }

            fclose($handle);

            // Unwrap long lines
            for ($a = 0; $a < count($lines[$titel[0]]); $a++) {
                if (substr($lines[$titel[0]][$a], 0, 3) == "333") {
                    $lines[$titel[0]][$a] = $lines[$titel[0]][$a - 1] . " " . $lines[$titel[0]][$a];
                    array_splice($lines[$titel[0]], $a - 1, 1);
                }
            }
            // clean up
            array_splice($lines[$titel[0]], 0, 3);
            array_splice($lines[$titel[0]], 10, 1);

            //remove empty NIL
            for ($a = 0; $a < count($lines[$titel[0]]); $a++) {
                if (stripos($lines[$titel[0]][$a], "nil") !== false) {
                    array_splice($lines[$titel[0]], $a, 1);
                }
            }

            //isolate wanted stations
            for ($a = 0; $a < count($lines[$titel[0]]); $a++) {
                // bits are 4 digit sensor values on one line
                $bits = explode(" ", $lines[$titel[0]][$a]);
                if (in_array($bits[0], $this->use)) {
                    if ($this->processBits($lines[$titel[0]][$a])) {
                        print " - done" . PHP_EOL;
                    }
                }
            }
        }
    }

    /**
     * @param $bits
     */
    private function processBits($bits) {
        $bits = explode(" ", $bits);
        $station = $bits[0];
        array_splice($bits, 0, 1);
        array_pop($bits);
        unset($json);
        unset($exists);
        $exists = null;
        $unit = null;

        // build insert values
        $json['date'] = $this->time;

        foreach ($this->bitIds as $key => $bp) {
            $json['st'] = $station; //weather station number

            if ($bits[3][1] == "9") {
                $dp = "R";
                $dp .= (int)(substr($bits[3], 2, 3));
            } elseif ($bits[3][1] == "1") {
                $dp = "-";
                $dp .= (int)(substr($bits[3], 2, 3)) / 10;
            } else {
                $dp = (int)(substr($bits[3], 2, 3)) / 10;
            }

            $json['cc'] = (int)($bits[0][1] == "/" ? "n/a" : (int)$bits[0][0]); // skydække
            $json['wd'] = $this->retninger[ceil((substr($bits[1], 1, 2) * 10) / 22.5)]; // vindretning
            $json['ws'] = (int)((int)substr($bits[1], 3, 2)); // vindhastighed
            $json['tp'] = $bits[2][1] == "1" ? "-" : "+"; //temp minus or plus
            $json['te'] = (int)(substr($bits[2], 2, 3)); //temp
            $json['dp'] = (string)$dp; //dugpunkt
            $json['sp'] = (string)(substr($bits[4], 1, 4) / 10); //stationstryk
            $json['op'] = (string)(substr($bits[5], 1, 4) / 10); //havtryk
            $json['pc'] = (string)(substr($bits[6], 2, 3) / 10); //trykændring
        }
        $indexData = $json;
        $update = $this->dmiCollection->update($indexData, $json, array('upsert' => TRUE, 'w' => TRUE));
        return $update['ok'];
    }

    /**
     * @param $fullName
     * @return string
     *
     */
    protected function createAbbrevationFromFullName($fullName) {
        $items = explode(' ', $fullName);
        $abbrevation = '';
        foreach ($items as $item) {
            $abbrevation .= $item[0];
        }
        return $abbrevation;
    }

    /**
     * @param string $pattern
     * @param int $flags
     * @param string $path
     * @return array
     */
    protected function rglob($pattern = '*', $flags = 0, $path = '') {
        $paths = glob($path . '*', GLOB_MARK | GLOB_ONLYDIR | GLOB_NOSORT);
        $files = glob($path . $pattern, $flags);
        foreach ($paths as $path) {
            $files = array_merge($files, self::rglob($pattern, $flags, $path));
        }
        return $files;
    }

}
