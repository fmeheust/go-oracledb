/*
** Copyright (c) 2026 Oracle and/or its affiliates.
**
** The Universal Permissive License (UPL), Version 1.0
**
** Subject to the condition set forth below, permission is hereby granted to any
** person obtaining a copy of this software, associated documentation and/or data
** (collectively the "Software"), free of charge and under any and all copyright
** rights in the Software, and any and all patent rights owned or freely
** licensable by each licensor hereunder covering either (i) the unmodified
** Software as contributed to or provided by such licensor, or (ii) the Larger
** Works (as defined below), to deal in both
**
** (a) the Software, and
** (b) any piece of software and/or hardware listed in the lrgrwrks.txt file if
** one is included with the Software (each a "Larger Work" to which the Software
** is contributed by such licensors),
**
** without restriction, including without limitation the rights to copy, create
** derivative works of, display, perform, and distribute the Software and make,
** use, sell, offer for sale, import, export, have made, and have sold the
** Software and the Larger Work(s), and to sublicense the foregoing rights on
** either these or other terms.
**
** This license is subject to the following condition:
** The above copyright notice and either this complete permission notice or at
** a minimum a reference to the UPL must be included in all copies or
** substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
 */

package session

import (
	"fmt"
	"os"
	"testing"

	oracleTest "github.com/oracle/go-oracledb/v26/internal/tests"
)

func TestMain(m *testing.M) {
	err := oracleTest.InitConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "InitConfig failed: %v\n", err)
		os.Exit(1)
	}
	TestEnvironement = oracleTest.TestEnvironement
	TestingConfig = oracleTest.TestingConfig
	DefaultTestConfig = oracleTest.DefaultTestConfig
	os.Exit(m.Run())
}

func TestCategoryExecutor(t *testing.T) {
	oracleTest.RunCategoryExecutor(t, oracleTest.TestCategories, testCases)
}

type Version = oracleTest.Version
type TestConfig = oracleTest.TestConfig
type TestingEnvironment = oracleTest.TestingEnvironment

var DefaultTestConfig *TestConfig
var TestEnvironement TestingEnvironment
var TestingConfig *TestConfig

var testCases = []oracleTest.CategorizedTestCase{
	{Name: "TestAcceptPacketClampsOversizedValues", Categories: "unitary", Exclusive: false, Fn: TestAcceptPacketClampsOversizedValues},
	{Name: "TestSessionAttsSetFromParsedDescriptionKeepsDefaultDNMatch", Categories: "unitary", Exclusive: false, Fn: TestSessionAttsSetFromParsedDescriptionKeepsDefaultDNMatch},
	{Name: "TestHeaderMarshalUnmarshal", Categories: "unitary", Exclusive: false, Fn: TestHeaderMarshalUnmarshal},
	{Name: "TestConnectPacketMarshal", Categories: "unitary", Exclusive: false, Fn: TestConnectPacketMarshal},
	{Name: "TestDataPacketMarshal", Categories: "unitary", Exclusive: false, Fn: TestDataPacketMarshal},
	{Name: "TestDataPacketFillBuf", Categories: "unitary", Exclusive: false, Fn: TestDataPacketFillBuf},
	{Name: "TestDataPacketPrepare2Send", Categories: "unitary", Exclusive: false, Fn: TestDataPacketPrepare2Send},
	{Name: "TestDataPacketReset", Categories: "unitary", Exclusive: false, Fn: TestDataPacketReset},
	{Name: "TestDataPacketUnmarshal", Categories: "unitary", Exclusive: false, Fn: TestDataPacketUnmarshal},
	{Name: "TestDataPacketRead", Categories: "unitary", Exclusive: false, Fn: TestDataPacketRead},
	{Name: "TestDataPacketRemaining", Categories: "unitary", Exclusive: false, Fn: TestDataPacketRemaining},
	{Name: "TestAcceptPacketUnmarshal", Categories: "unitary", Exclusive: false, Fn: TestAcceptPacketUnmarshal},
	{Name: "TestAcceptPacketUnmarshalUnsupportedCompressionScheme", Categories: "unitary", Exclusive: false, Fn: TestAcceptPacketUnmarshalUnsupportedCompressionScheme},
	{Name: "TestRefusePacketUnmarshal", Categories: "unitary", Exclusive: false, Fn: TestRefusePacketUnmarshal},
	{Name: "TestRedirectPacketUnmarshal", Categories: "unitary", Exclusive: false, Fn: TestRedirectPacketUnmarshal},
	{Name: "TestMarkerPacket", Categories: "unitary", Exclusive: false, Fn: TestMarkerPacket},
	{Name: "TestControlPacketUnmarshal", Categories: "unitary", Exclusive: false, Fn: TestControlPacketUnmarshal},
	{Name: "TestResendPacketUnmarshal", Categories: "unitary", Exclusive: false, Fn: TestResendPacketUnmarshal},
	{Name: "TestContains", Categories: "unitary", Exclusive: false, Fn: TestContains},
	{Name: "TestGenUUID", Categories: "unitary", Exclusive: false, Fn: TestGenUUID},
	{Name: "TestNewSessionAttsDefaults", Categories: "unitary", Exclusive: false, Fn: TestNewSessionAttsDefaults},
	{Name: "TestSessionAttsSetFromDescription", Categories: "unitary", Exclusive: false, Fn: TestSessionAttsSetFromDescription},
	{Name: "TestSessionAttsSetFromNil", Categories: "unitary", Exclusive: false, Fn: TestSessionAttsSetFromNil},
	{Name: "TestSessionAttsSetFromCompressionDefaultLevel", Categories: "unitary", Exclusive: false, Fn: TestSessionAttsSetFromCompressionDefaultLevel},
	{Name: "TestSessionAttsSetFromCompressionThreshold", Categories: "unitary", Exclusive: false, Fn: TestSessionAttsSetFromCompressionThreshold},
	{Name: "TestSessionAttsSetFromEnableNotBroken", Categories: "unitary", Exclusive: false, Fn: TestSessionAttsSetFromEnableNotBroken},
	{Name: "TestSessionAttsSetFromSSLAllowWeakDNMatchNo", Categories: "unitary", Exclusive: false, Fn: TestSessionAttsSetFromSSLAllowWeakDNMatchNo},
	{Name: "TestSessionAttsSetFromUseSNINo", Categories: "unitary", Exclusive: false, Fn: TestSessionAttsSetFromUseSNINo},
	{Name: "TestReadWalletFile", Categories: "unitary", Exclusive: false, Fn: TestReadWalletFile},
	{Name: "TestReadWalletFileError", Categories: "unitary", Exclusive: false, Fn: TestReadWalletFileError},
	{Name: "TestPrepare", Categories: "unitary", Exclusive: false, Fn: TestPrepare},
	{Name: "TestPrepareWithWallet", Categories: "unitary", Exclusive: false, Fn: TestPrepareWithWallet},
	{Name: "TestPrepareWithSystemTrust", Categories: "unitary", Exclusive: false, Fn: TestPrepareWithSystemTrust},
	{Name: "TestPrepareDefaultTimeout", Categories: "unitary", Exclusive: false, Fn: TestPrepareDefaultTimeout},
	{Name: "TestPrepareWalletError", Categories: "unitary", Exclusive: false, Fn: TestPrepareWalletError},
	{Name: "TestPrepareUUIDError", Categories: "unitary", Exclusive: false, Fn: TestPrepareUUIDError},
	{Name: "TestPrepareNoPrefix", Categories: "unitary", Exclusive: false, Fn: TestPrepareNoPrefix},
	{Name: "TestPrepareNonTCPSNoWalletLoad", Categories: "unitary", Exclusive: false, Fn: TestPrepareNonTCPSNoWalletLoad},
	{Name: "TestPrepareClampsSDU", Categories: "unitary", Exclusive: false, Fn: TestPrepareClampsSDU},
	{Name: "TestPacketStreamerRoutesDataAcrossAPause", Categories: "unitary", Exclusive: false, Fn: TestPacketStreamerRoutesDataAcrossAPause},
	{Name: "TestPacketStreamerBlocksAfterMarkerUntilAcknowledged", Categories: "unitary", Exclusive: false, Fn: TestPacketStreamerBlocksAfterMarkerUntilAcknowledged},
	{Name: "TestPacketStreamerWritesThroughTheSingleWriter", Categories: "unitary", Exclusive: false, Fn: TestPacketStreamerWritesThroughTheSingleWriter},
	{Name: "TestPacketStreamerRoutesControlPackets", Categories: "unitary", Exclusive: false, Fn: TestPacketStreamerRoutesControlPackets},
	{Name: "TestPacketStreamerMakesReaderErrorsTerminal", Categories: "unitary", Exclusive: false, Fn: TestPacketStreamerMakesReaderErrorsTerminal},
	{Name: "TestNegotiationStreamerReadsAndWritesInWireOrder", Categories: "unitary", Exclusive: false, Fn: TestNegotiationStreamerReadsAndWritesInWireOrder},
	{Name: "TestNegotiationStreamerRejectsInvalidDependencies", Categories: "unitary", Exclusive: false, Fn: TestNegotiationStreamerRejectsInvalidDependencies},
	{Name: "TestNegotiationStreamerPropagatesReadFailures", Categories: "unitary", Exclusive: false, Fn: TestNegotiationStreamerPropagatesReadFailures},
	{Name: "TestDataServicePacketizesAndFlushesWrites", Categories: "unitary", Exclusive: false, Fn: TestDataServicePacketizesAndFlushesWrites},
	{Name: "TestDataServiceReadsFromTheRuntimeStreamer", Categories: "unitary", Exclusive: false, Fn: TestDataServiceReadsFromTheRuntimeStreamer},
	{Name: "TestBreakResetStatePreventsOverlappingExchanges", Categories: "unitary", Exclusive: false, Fn: TestBreakResetStatePreventsOverlappingExchanges},
	{Name: "TestBreakResetServiceCancelsAndResumesData", Categories: "unitary", Exclusive: false, Fn: TestBreakResetServiceCancelsAndResumesData},
	{Name: "TestBreakResetServiceRespondsToServerBreak", Categories: "unitary", Exclusive: false, Fn: TestBreakResetServiceRespondsToServerBreak},
	{Name: "TestNotificationServiceConsumesControlNotifications", Categories: "unitary", Exclusive: false, Fn: TestNotificationServiceConsumesControlNotifications},
	{Name: "TestNetworkSessionWiresRuntimeServices", Categories: "unitary", Exclusive: false, Fn: TestNetworkSessionWiresRuntimeServices},
	{Name: "TestConnectionNegotiatorStartsWithoutRuntimeServices", Categories: "unitary", Exclusive: false, Fn: TestConnectionNegotiatorStartsWithoutRuntimeServices},
	{Name: "TestConnectionNegotiatorTransfersAcceptedTransport", Categories: "unitary", Exclusive: false, Fn: TestConnectionNegotiatorTransfersAcceptedTransport},
}
