<?php
/**
 * Nsq Tcp 客户端
 * @author Janson
 * @create 2016-04-22
 */

namespace App\Helper\Nsq;
use App\Helper\Log;
use nsqphp\Nsq;

class NsqTcpClient {
	private $_host;
	private $_port;
	private $_topic;
	private $_nsh = null;			//nsq socket handle
	private $_retryTimes = 1;		//重试次数
	private $_connectionTimeout = 3;
	private $_readWriteTimeout = 3;	//读写时长  单位：秒

	public $logPath = SERVICE_LOG_PATH . 'nsq/';

	private static $_userId;
	private static $_corpId;
	private static $_instances = array();

	private function __construct($module) {
		$this->_module = $module;
		$this->getNsqPhp();	//获取nsqphp实例
	}

	/**
	 * 获取nsq client单例
	 * @param string $module	nsq服务模块key
	 * @param int $userId		员工ID
	 * @param int $corpId		企业ID
	 * @return mixed
	 * @throws \Exception
	 */
	public static function getInstance($module, $userId, $corpId) {
		try {
			if (empty($module)) {
				throw new \InvalidArgumentException('NsqTcpClient: module key can not be empty');
			}

			if ($userId > 0 && $corpId > 0) {
				self::$_userId = $userId;
				self::$_corpId = $corpId;
			} else {
				throw new \InvalidArgumentException('NsqTcpClient: userId or corpId is wrong');
			}

			if ( ! isset(self::$_instances[$module])) {
				self::$_instances[$module] = new self($module);
			}
		} catch(\Exception $e) {
			Log::error(
				'NsqTcpClient',
				SERVICE_LOG_PATH . 'nsq/error.log',
				$e->getMessage(),
				array(
					'module' => $module,
					'userId' => $userId,
					'corpId' => $corpId
				)
			);

			throw $e;
		}

		return self::$_instances[$module];
	}

	/**
	 * 一次发布单条nsq消息
	 * @param array $nsqData
	 * @return bool
	 */
	public function pub($nsqData) {
		return $this->doPub('publish', $nsqData);
	}

	/**
	 * 一次发布多条nsq消息
	 * @param array $nsqDatas
	 * @return bool
	 */
	public function mpub($nsqDatas) {
		return $this->doPub('mpublish', $nsqDatas);
	}

	/**
	 * 执行nsq消息发布
	 * @param string $cmd
	 * @param array $nsqDatas
	 */
	private function doPub($cmd, $nsqDatas) {
		$requestId = uniqid();

		try{
			if(empty($nsqDatas) || ! is_array($nsqDatas)) {
				throw new \InvalidArgumentException('NsqTcpClient: nsq data is empty or not an array');
			}

			$clientInfo = array(
				'module' => $this->_module,
				'userId' => self::$_userId,
				'corpId' => self::$_corpId,
				'host' => $this->_host,
				'port' => $this->_port,
				'topic' => $this->_topic,
				'cmd' => $cmd,
				'nsqDatas' => $nsqDatas
			);

			if ($cmd == 'mpublish') {
				foreach ($nsqDatas as $item) {
					$message[] = json_encode($item);
				}
			}
			else {
				$message = json_encode($nsqDatas);
			}

			$start = microtime(true);
			for ($i = 0; $i <= $this->_retryTimes; $i++) {
				try {
					$this->_nsh->$cmd($this->_topic, $message);
					$clientInfo['RequestTime'] = microtime(true) - $start;

					Log::info(
						'NsqTcpClient',
						$this->logPath . 'request.log',
						"RequestId: $requestId",
						$clientInfo
					);

					$result = true;
					break;
				} catch (\Exception $e) {
					if ($i < $this->_retryTimes) {
						Log::error(
							'NsqTcpClient',
							$this->logPath . 'retry.log',
							"RequestId: $requestId, Times: " . ($i + 1) . ", Error: post nsq message fail",
							$clientInfo
						);
					} else {
						throw $e;
					}
				}
			}
		} catch(\Exception $e) {
			Log::error(
				'NsqTcpClient',
				$this->logPath . 'error.log',
				"RequestId: $requestId, Error: {$e->getMessage()}",
				$clientInfo
			);

			throw $e;
		}

		return $result;
	}

	/**
	 * 新建nsqphp\Nsq实例
	 */
	private function getNsqPhp() {
		$this->config();	//加载配置

		//初始化一个nsqphp\Nsq实例
		$this->_nsh = new Nsq(NULL, NULL, NULL, NULL, $this->_connectionTimeout, $this->_readWriteTimeout);
		$this->_nsh->publishTo("{$this->_host}:{$this->_port}");
	}

	/**
	 * 获取Nsq服务模块配置
	 * @throws \Exception
	 */
	private function config() {
		$nsq_config = \Zend_Registry::get('nsq_config');

		if( ! isset($nsq_config[$this->_module])) {
			throw new \InvalidArgumentException('NsqTcpClient: module key is wrong');
		}

		if(empty($nsq_config[$this->_module]) || ! is_array($nsq_config[$this->_module])) {
			throw new \InvalidArgumentException('NsqTcpClient: Nsq config is empty or not an array');
		}

		if( ! isset($nsq_config[$this->_module]['host']) || ! isset($nsq_config[$this->_module]['tcp_port']) || ! isset($nsq_config[$this->_module]['topic'])) {
			throw new \InvalidArgumentException('NsqTcpClient: Nsq host, tcp_port or topic is not set');
		}

		$this->_host = $nsq_config[$this->_module]['host'];
		$this->_port = $nsq_config[$this->_module]['tcp_port'];
		$this->_topic = $nsq_config[$this->_module]['topic'];

		if(isset($nsq_config[$this->_module]['log_path'])) {
			$this->logPath = $nsq_config[$this->_module]['log_path'];
		}

		if(isset($nsq_config[$this->_module]['connection_timeout'])) {
			$this->_connectionTimeout = intval($nsq_config[$this->_module]['connection_timeout']);
		}

		if(isset($nsq_config[$this->_module]['readwrite_timeout'])) {
			$this->_readWriteTimeout = intval($nsq_config[$this->_module]['readwrite_timeout']);
		}
	}

	/**
	 * 防止克隆
	 */
	private function __clone() {

	}
}